const year = "2018";

const folderraiz = '/img/flex/img/noticias/'+year;
const fs = require('fs');

const fsp = require('fs-promise');

//###########################
const AWS = require('ibm-cos-sdk');
const async = require('async');

//Definicion de parametros de conexcion al repositorio de IBM.
//s3.public.us-south.cloud-object-storage.appdomain.cloud
const config_ibm = {
    endpoint: 's3.us-south.cloud-object-storage.appdomain.cloud',
    apiKeyId: 'FbxO8sTQKXiUJsk5TmoSLbuoLCaZ0bzotpSgW8jmND8Z',
    ibmAuthEndpoint: 'https://iam.cloud.ibm.com/identity/token',
    serviceInstanceId: 'crn:v1:bluemix:public:cloud-object-storage:global:a/9e55c33c55e46456f715f8f244b9e33e:ab86c1bd-afec-48c1-b8c6-9b5cb4a9eacb::',
};

//Definicion del objeto ibm
const cos = new AWS.S3(config_ibm);
const mes = "04";


let uploadFileIbm = (bucketName, itemName, filePath) => {

    let resp = "";
    
    return new Promise((resolve, reject) => {
	
        var uploadID = null;
        
        console.log(bucketName);
        console.log(itemName);
        console.log(filePath);

		if (!fs.existsSync(filePath)) {
            resp = {"status":"fail", "coderr":"001","message":"Err, not exist file.","bucket":"","filename":""};
            reject(resp);
		}

        console.log(`Starting multi-part upload for ${itemName} to bucket: ${bucketName}`);
            
        cos.createMultipartUpload({
            Bucket: bucketName,
            Key: itemName
        }).promise()
        .then((data) => {
            
            uploadID = data.UploadId;

            //begin the file upload        
            fs.readFile(filePath, (e, fileData) => {
                
                //min 5MB part
                var partSize = 1024 * 1024 * 5;
                var partCount = Math.ceil(fileData.length / partSize);

                async.timesSeries(partCount, (partNum, next) => {
                    var start = partNum * partSize;
                    var end = Math.min(start + partSize, fileData.length);

                    partNum++;

                    console.log(`Uploading to ${itemName} (part ${partNum} of ${partCount})`);  

                    cos.uploadPart({
                        Body: fileData.slice(start, end),
                        Bucket: bucketName,
                        Key: itemName,
                        PartNumber: partNum,
                        UploadId: uploadID
                    }).promise()
                    .then((data) => {
                        next(e, {ETag: data.ETag, PartNumber: partNum});
                    })
                    .catch((e) => {
                        resp = {"status":"fail", "coderr":"002","message":"Err, upload file to IBM.","bucket":"","filename":""};
                        reject(resp);                      
                    });
                }, (e, dataPacks) => {
                    cos.completeMultipartUpload({
                        Bucket: bucketName,
                        Key: itemName,
                        MultipartUpload: {
                            Parts: dataPacks
                        },
                        UploadId: uploadID
                    }).promise()
                    .then(() => {
                            
                        resp = {status:"ok", coderr:"",message:".",bucket:bucketName,filename:itemName};
                        resolve(resp);
                    })
                    .catch((e) => {
                        resp = {"status":"fail", "coderr":"003","message":"Err, upload file to IBM.","bucket":"","filename":""};
                        reject(resp);  
                    });
                });
            });
        })
        .catch((e) => {
            
            resp = {"status":"fail", "coderr":"004","message":"Err, conexion to IBM."+e,"bucket":"","filename":""};
            reject(resp);            
        });		
            
   
		
	
	});

}


function showTime() {
  var timeNow = new Date();
  var hours   = timeNow.getHours();
  var minutes = timeNow.getMinutes();
  var seconds = timeNow.getSeconds();
  var timeString = "" + ((hours > 12) ? hours - 12 : hours);
  timeString  += ((minutes < 10) ? ":0" : ":") + minutes;
  timeString  += ((seconds < 10) ? ":0" : ":") + seconds;
  timeString  += (hours >= 12) ? " P.M." : " A.M.";
  return timeString;
}

function getdatenow() {
	
	var today = new Date();
	var today_ = today;
				
	var dd = today_.getDate();
	var mm = today_.getMonth()+1;
	var yyyy = today_.getFullYear();

	if(dd<10) {
		dd = '0'+dd;
	} 

	if(mm<10) {
		mm = '0'+mm;
	} 

	var today_ = yyyy+""+mm+""+dd;
	
	return today_;
}

(async () => {

  try {

	var hora_envio = showTime();	
	var today_ = getdatenow(); 

	//se debe se escribir si el mes ya acabo su bucle
	
	fs.appendFile("log_x_mes/"+year+mes+"_ini.txt", ' inicio de carga > ' +today_+' ! '+hora_envio+"\n\n", function (err) {
		if (err) throw err;
		console.log('Saved!');
	});		

	let contacarpetaok = 0;
	let contacarpetafail = 0;
    
	const repoxdias = await fsp.readdir(folderraiz);

	let tot_items = repoxdias.length
	var i = 0;
	
	for(i=0;i<tot_items;i++){
		
		if(/201804/i.test(repoxdias[i])){
		
			if (fs.existsSync("log/"+repoxdias[i]+"_end.txt")) {
				console.log('la carpeta '+repoxdias[i]+' ya fue cargada al 100%.');
			}else{
				
				var hora_envio = showTime();	
				var today_ = getdatenow(); 
				
				const repoxdiasximg = await fsp.readdir(folderraiz+'/'+repoxdias[i]);
				
				console.log('carpeta > '+repoxdias[i]);
				console.log('Cantidad de archivos > '+repoxdiasximg.length);
				
				fs.writeFile("log/"+repoxdias[i]+".txt", 'Cantidad de archivos > '+repoxdiasximg.length + ' > ' +today_+' ! '+hora_envio, function(err) {
					if(err) {
						return console.log(err);
					}
					
					console.log("The file was saved!");
				});	

				//vacear log  insert y error	
				fs.writeFile("log/"+repoxdias[i]+"_items_cargado.txt", '', function(err) {
					if(err) {
						return console.log(err);
					}
					
					console.log("The file was saved!");
				});

				fs.writeFile("log/"+repoxdias[i]+"_items_no_cargado.txt", '', function(err) {
					if(err) {
						return console.log(err);
					}
					
					console.log("The file was saved!");
				});				
				
				
				let tot_itemsimg = repoxdiasximg.length;
				let tot_itemsimg_end = repoxdiasximg.length-1;
				let a = 0;
				let canterror = 0;
				for(a=0;a<tot_itemsimg;a++){
					
					//if(a<3){
					var hora_envio = showTime();	
					var today_ = getdatenow(); 	
					
					console.log('archivo > '+repoxdiasximg[a]);
					
					try{
												
						//####### Carga de IBM ################
						lista_b = "img/noticias/"+year+"/"+repoxdias[i]+"/"+repoxdiasximg[a]
						let ruta_upload_ibm = lista_b;
																		
						var patron_f = '/';
						var re_name_items_ibm = new RegExp(patron_f, 'g');
																																					
						let ruta_upload_ibm_pre = ruta_upload_ibm.replace(re_name_items_ibm,"_");
															
						let bucketName = "fleximgnews";
						let itemName = ruta_upload_ibm_pre;
						let filePath = folderraiz+"/"+repoxdias[i]+"/"+repoxdiasximg[a]
																		
						let resp = await uploadFileIbm(bucketName, itemName, filePath).then(url_meta => {	
							return url_meta;
						})
						.then(resp => {
							return resp;
						})
						.catch(err => {
							return err;	
						});
													
						console.log("resultado de carga de IBM:");				
						console.log(resp.status);
																		
						//#######
						if(resp.status=="ok"){
							fs.appendFile("log/"+repoxdias[i]+"_items_cargado.txt", itemName+"\n", function (err) {
							  if (err) throw err;
							  console.log('Saved!');
							});						
						}else{
							
							canterror = canterror + 1;
							
							fs.appendFile("log/"+repoxdias[i]+"_items_no_cargado.txt", filePath+"\n", function (err) {
							  if (err) throw err;
							  console.log('Saved!');
							});							
						}
					}catch(error){
						
						console.log(error);
						
						canterror = canterror + 1;
						
						fs.appendFile("log/"+repoxdias[i]+"_items_no_cargado.txt", filePath+"\n", function (err) {
						  if (err) throw err;
						  console.log('Saved!');
						});						
					
					}
					
					if(tot_itemsimg_end==a){
						
						if(canterror==0){

							contacarpetaok = contacarpetaok + 1;
							
							fs.writeFile("log/"+repoxdias[i]+"_end.txt", 'OK > ' +today_+' ! '+hora_envio, function(err) {
								if(err) {
									return console.log(err);
								}
								
								console.log("The file was saved!");
							});
						
						}else{
							contacarpetafail = contacarpetafail + 1;
							

						}
					
					}
					
					//}
				}
			}
		}	
	}

	var hora_envio = showTime();	
	var today_ = getdatenow(); 

	//se debe se escribir si el mes ya acabo su bucle
	
	fs.appendFile("log_x_mes/"+year+mes+".txt", 'Cant carpetas cargadas 100% '+contacarpetaok+' > Cant carpetas no cargadas al 100% '+contacarpetafail+' > ' +today_+' ! '+hora_envio+"\n\n", function (err) {
		if (err) throw err;
		console.log('Saved!');
	});	

	
  } catch (e) {
    console.log('error: ', e);
  }
  
})();


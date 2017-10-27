/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.septeni.javaaws4s3;

/**
 *
 * @author lam_nt
 */
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class S3Client {

    private final static String BUCKET_NAME = "p4l-development";

    private static List<String> getListObjects() {
        List<String> objectList = new ArrayList();
        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            ObjectListing ol = s3Client.listObjects(BUCKET_NAME);
            List<S3ObjectSummary> objects = ol.getObjectSummaries();
            for (S3ObjectSummary os : objects) {
                objectList.add(os.getKey());
            }

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which"
                    + " means your request made it "
                    + "to Amazon S3, but was rejected with an error response"
                    + " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"
                    + " the client encountered "
                    + "an internal error while trying to "
                    + "communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        return objectList;
    }

    private static void downloadObject(String key_name, String destinationFileName) {
        final AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
            S3Object o = s3.getObject(BUCKET_NAME, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File(destinationFileName));
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        //show files and driectories on S3
        List<String> objects = getListObjects();
        System.out.println(objects);
        
        //download 1 file
        String keyOnS3 = "lap_production/report_ad_medias/2017/10/p4l_report_admedia_20171022_24_01_1256.tsv";
        String destinationFileName = "/tmp/p4l_report_admedia_20171022_24_01_1256.tsv";
        downloadObject(keyOnS3, destinationFileName);
    }
}

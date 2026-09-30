SELECT CASE WHEN sqft >= 17492 THEN 'high' WHEN sqft >= 4646 THEN 'mid' ELSE 'low' END AS sqft_bucket, COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count FROM stores GROUP BY sqft_bucket;

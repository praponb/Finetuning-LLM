SELECT id, region, CAST(sqft AS STRING) AS sqft_str, CAST(sqft AS DOUBLE) AS sqft_str_2 FROM stores WHERE store_name LIKE '%Flagship%' AND region IS NULL

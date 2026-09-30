SELECT id, store_name, CAST(sqft AS DOUBLE) AS sqft_num FROM stores WHERE sqft >= 10801 ORDER BY id DESC, sqft_num

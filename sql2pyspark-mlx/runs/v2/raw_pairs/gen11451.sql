SELECT id, region, city, opened_date, CAST(sqft AS STRING) AS sqft_str FROM stores WHERE region != 'South' ORDER BY opened_date

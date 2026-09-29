SELECT opened_date, sqft, region, CONCAT_WS('-', city, store_name) AS city_store_name_key, CAST(sqft AS STRING) AS sqft_str FROM stores

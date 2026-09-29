SELECT city, region, store_name, INITCAP(TRIM(region)) AS region_title, ROUND(sqft * 0.85, 2) AS sqft_scaled FROM stores WHERE sqft BETWEEN 4531 AND 5665

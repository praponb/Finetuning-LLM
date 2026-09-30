SELECT city, ROUND(sqft * 1.15, 2) AS sqft_scaled FROM stores WHERE city = 'Bangkok' AND LOWER(TRIM(store_name)) = 'mall central';

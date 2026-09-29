WITH top_stores AS (SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) AS rn FROM stores WHERE LOWER(TRIM(store_name)) = 'mall central') SELECT * FROM top_stores WHERE rn <= 1

WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) AS rn FROM stores WHERE region = 'Central') SELECT * FROM numbered WHERE rn <= 3 ORDER BY store_name

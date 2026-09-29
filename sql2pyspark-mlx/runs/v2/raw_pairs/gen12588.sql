WITH ranked_stores AS (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) AS rn FROM stores) SELECT id, city, sqft, region FROM ranked_stores WHERE rn <= 5 ORDER BY city

WITH ranked_stores AS (SELECT id, city, sqft, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn FROM stores) SELECT id, city, sqft FROM ranked_stores WHERE rn <= 3 ORDER BY sqft DESC

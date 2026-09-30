WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS pos FROM stores) SELECT id, city, sqft FROM ranked_stores WHERE pos <= 2 ORDER BY sqft, city

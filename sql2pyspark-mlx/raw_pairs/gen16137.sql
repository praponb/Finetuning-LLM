WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) pos FROM stores) SELECT id, city, sqft, region FROM ranked_stores WHERE pos <= 2 ORDER BY sqft DESC

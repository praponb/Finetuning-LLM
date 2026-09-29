WITH ranked_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos FROM stores) SELECT * FROM ranked_stores WHERE pos <= 3 ORDER BY pos DESC;

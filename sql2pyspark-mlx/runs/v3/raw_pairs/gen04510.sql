WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rnk FROM stores) SELECT * FROM top_stores WHERE rnk <= 5 ORDER BY sqft DESC;

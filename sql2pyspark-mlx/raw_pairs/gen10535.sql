WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS rnk FROM stores) SELECT id, region, sqft FROM top_stores WHERE rnk <= 3;

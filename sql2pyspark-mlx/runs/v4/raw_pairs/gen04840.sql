WITH top_stores AS (SELECT id, region, sqft, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS row_num FROM stores) SELECT * FROM top_stores WHERE row_num <= 2

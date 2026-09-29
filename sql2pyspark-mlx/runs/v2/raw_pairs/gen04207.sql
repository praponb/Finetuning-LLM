SELECT * FROM (SELECT id, region, sqft, city, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos FROM stores) AS r WHERE pos <= 3

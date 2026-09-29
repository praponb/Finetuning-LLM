WITH numbered AS (SELECT id, region, sqft, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS rn FROM stores) SELECT id, region, sqft FROM numbered WHERE rn = 1 ORDER BY id DESC;

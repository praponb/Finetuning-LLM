WITH numbered AS (SELECT id, city, sqft, store_name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS row_num FROM stores) SELECT id, city, sqft, store_name FROM numbered WHERE row_num = 1;

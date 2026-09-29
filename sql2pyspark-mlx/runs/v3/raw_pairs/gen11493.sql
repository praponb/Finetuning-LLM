WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS row_num FROM products) SELECT id, brand, price FROM numbered WHERE row_num <= 3

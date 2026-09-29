WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS row_num FROM products) SELECT * FROM numbered WHERE row_num <= 5;

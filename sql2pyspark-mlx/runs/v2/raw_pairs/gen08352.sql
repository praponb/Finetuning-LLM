SELECT * FROM (SELECT id, category, price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS row_num FROM products) AS t WHERE row_num <= 5

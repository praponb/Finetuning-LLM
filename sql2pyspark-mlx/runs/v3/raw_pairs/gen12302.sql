SELECT id, category, cost, price FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS row_num FROM products) AS r WHERE row_num = 1 ORDER BY price DESC

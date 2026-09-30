SELECT id, category, cost FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS row_num FROM products) AS r WHERE row_num <= 2 ORDER BY category;

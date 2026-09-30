SELECT * FROM (SELECT id, supplier_id, price, category, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS row_num FROM products) AS r WHERE row_num <= 2;

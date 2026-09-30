SELECT id, supplier_id, price FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY price DESC) AS pos FROM products WHERE supplier_id IS NULL) AS t WHERE pos = 1

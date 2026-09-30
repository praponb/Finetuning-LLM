SELECT * FROM (SELECT id, supplier_id, stock_qty, RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC) AS rn FROM products) AS r WHERE rn <= 3 ORDER BY rn DESC;

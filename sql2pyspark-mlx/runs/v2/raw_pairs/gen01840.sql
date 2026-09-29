SELECT id, supplier_id, price, MIN(price) OVER (PARTITION BY supplier_id) AS supplier_id_min_price FROM products;

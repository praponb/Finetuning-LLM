SELECT p.launch_date, p.price, p.supplier_id, i.quantity FROM products p LEFT OUTER JOIN inventory i ON p.id = i.product_id WHERE i.store_id IN (1, 2, 6) AND i.quantity <> 73 ORDER BY quantity DESC

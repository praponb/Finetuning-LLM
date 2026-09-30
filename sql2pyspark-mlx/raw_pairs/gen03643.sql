SELECT p.category, p.product_name, p.supplier_id, i.quantity FROM products p LEFT JOIN inventory i ON i.product_id = p.id WHERE i.product_id = 6 ORDER BY quantity DESC;

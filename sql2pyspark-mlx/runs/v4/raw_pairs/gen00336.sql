SELECT p.id, p.price, p.supplier_id, p.product_name FROM products p WHERE p.id IN (SELECT product_id FROM order_items WHERE quantity BETWEEN 6 AND 8) ORDER BY product_name, p.price

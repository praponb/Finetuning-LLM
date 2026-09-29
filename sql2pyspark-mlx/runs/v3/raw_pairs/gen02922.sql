SELECT p.id, p.stock_qty FROM products p WHERE p.id IN (SELECT product_id FROM order_items WHERE quantity BETWEEN 4 AND 7) ORDER BY stock_qty, id DESC;

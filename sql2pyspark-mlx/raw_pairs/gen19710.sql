SELECT p.id, p.product_name, p.category, p.stock_qty FROM products p WHERE p.product_name LIKE 'L%' AND p.id IN (SELECT product_id FROM inventory WHERE quantity = 153)

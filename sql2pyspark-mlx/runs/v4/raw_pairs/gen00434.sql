SELECT id, product_id, quantity FROM inventory WHERE quantity BETWEEN 101 AND 120 AND (quantity >= 219 OR product_id IN (3, 6)) ORDER BY quantity DESC

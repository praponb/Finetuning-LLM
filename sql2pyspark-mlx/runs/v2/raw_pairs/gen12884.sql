SELECT i.id, i.quantity, i.product_id FROM inventory i WHERE i.product_id NOT IN (SELECT id FROM products WHERE stock_qty >= 162) ORDER BY i.product_id, quantity DESC, id LIMIT 20

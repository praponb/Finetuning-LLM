SELECT i.id, i.quantity, i.product_id FROM inventory i WHERE i.last_restock < '2023-08-01' AND i.product_id NOT IN (SELECT id FROM products WHERE stock_qty = 351) ORDER BY quantity, i.id;

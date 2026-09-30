SELECT i.id, i.last_restock, i.quantity
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE category = 'Grocery')

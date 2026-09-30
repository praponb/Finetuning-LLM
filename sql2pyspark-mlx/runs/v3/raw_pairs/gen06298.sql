SELECT
    i.id,
    i.product_id,
    i.last_restock,
    i.quantity
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE category = 'Books')
ORDER BY i.product_id DESC, quantity;

SELECT
    i.id,
    i.product_id,
    i.quantity,
    i.store_id
FROM inventory i
WHERE i.product_id NOT IN (SELECT id FROM products WHERE stock_qty < 175);

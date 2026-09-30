SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE stock_qty > 338)

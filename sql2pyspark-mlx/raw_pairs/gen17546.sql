SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.quantity
FROM inventory i
WHERE i.product_id = 1 AND i.store_id IN (SELECT id FROM stores WHERE YEAR(opened_date) = 2025);

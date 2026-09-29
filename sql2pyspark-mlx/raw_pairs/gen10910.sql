SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.product_id
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE opened_date < '2025-09-01');

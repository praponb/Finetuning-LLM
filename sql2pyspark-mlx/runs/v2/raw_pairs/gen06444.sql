SELECT
    i.product_id,
    i.last_restock,
    i.store_id,
    s.store_name,
    s.opened_date
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.last_restock < '2025-09-07'
ORDER BY s.store_name DESC, opened_date;

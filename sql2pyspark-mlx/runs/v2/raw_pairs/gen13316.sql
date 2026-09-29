SELECT
    i.store_id,
    i.product_id,
    i.last_restock,
    s.opened_date,
    s.region
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE s.opened_date > '2023-07-01';

SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.product_id,
    s.region,
    s.opened_date,
    s.city
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
WHERE s.region != 'Central'

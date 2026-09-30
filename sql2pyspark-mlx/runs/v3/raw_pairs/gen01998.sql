SELECT
    i.id,
    i.last_restock,
    s.opened_date,
    s.region,
    p.brand
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
LEFT JOIN products p ON p.id = i.product_id
WHERE i.store_id IN (3, 5) AND s.store_name LIKE 'Ai%'
ORDER BY brand DESC, id
LIMIT 50;

SELECT
    i.id,
    i.product_id,
    i.last_restock,
    s.city,
    s.region,
    p.cost,
    p.launch_date
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
LEFT JOIN products p ON i.product_id = p.id
WHERE s.store_name LIKE '%River%'
ORDER BY region;

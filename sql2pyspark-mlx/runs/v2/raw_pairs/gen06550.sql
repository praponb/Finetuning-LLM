SELECT
    i.id,
    i.store_id,
    i.quantity,
    p.product_name,
    p.launch_date,
    s.store_name,
    s.opened_date
FROM inventory i
LEFT OUTER JOIN products p ON p.id = i.product_id
JOIN stores s ON i.store_id = s.id
ORDER BY s.opened_date

SELECT
    i.id,
    i.quantity,
    s.opened_date,
    s.region,
    p.product_name,
    p.category
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
WHERE p.cost IS NOT NULL
ORDER BY i.id DESC

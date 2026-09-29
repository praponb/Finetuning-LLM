SELECT
    i.product_id,
    p.category,
    p.price,
    s.opened_date,
    s.region
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
WHERE s.region != 'South' AND i.quantity >= 210
ORDER BY s.opened_date;

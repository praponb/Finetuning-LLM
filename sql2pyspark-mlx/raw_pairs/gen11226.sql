SELECT
    i.product_id,
    i.store_id,
    s.sqft,
    s.region,
    p.launch_date
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
LEFT JOIN products p ON i.product_id = p.id
ORDER BY p.launch_date DESC, product_id DESC;

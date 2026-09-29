SELECT
    i.id,
    i.quantity,
    p.brand,
    p.price,
    s.sqft
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
LEFT JOIN stores s ON i.store_id = s.id
WHERE s.city = 'Berlin' OR s.store_name IS NULL

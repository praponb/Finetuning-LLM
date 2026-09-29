SELECT
    i.id,
    i.product_id,
    p.price,
    s.sqft,
    s.store_name
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
WHERE s.city = 'Chiang Mai'

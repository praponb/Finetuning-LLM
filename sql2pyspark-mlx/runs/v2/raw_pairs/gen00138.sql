SELECT
    i.id,
    i.store_id,
    i.product_id,
    i.quantity,
    s.region
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE s.sqft <= 15723

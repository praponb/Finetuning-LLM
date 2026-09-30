SELECT
    oi.id,
    oi.order_id,
    oi.unit_price,
    o.store_id,
    o.status,
    s.store_name,
    s.sqft,
    COALESCE(oi.quantity, 0) AS quantity_filled
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN stores s ON s.id = o.store_id
WHERE NOT (o.status IN ('PENDING', 'COMPLETED') OR s.region IN ('North', 'Central'))
ORDER BY id DESC;

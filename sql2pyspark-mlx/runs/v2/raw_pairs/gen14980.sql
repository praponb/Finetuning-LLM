SELECT
    oi.id,
    oi.unit_price,
    o.amount,
    s.region,
    s.sqft
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
LEFT JOIN stores s ON o.store_id = s.id
WHERE s.city IN ('Bangkok', 'Tokyo', 'Berlin')
ORDER BY oi.id DESC, s.region

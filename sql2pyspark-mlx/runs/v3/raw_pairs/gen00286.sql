SELECT
    p.id,
    p.method,
    p.paid_at,
    o.amount,
    o.store_id,
    s.sqft,
    UPPER(o.status) AS status_upper
FROM payments p
JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
WHERE s.sqft != 10426 OR s.sqft >= 12479

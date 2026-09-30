SELECT
    o.order_date,
    p.paid_at,
    p.amount,
    p.order_id,
    CASE WHEN p.amount >= 1360 THEN 'High' WHEN p.amount >= 340 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE p.method IS NULL

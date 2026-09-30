SELECT
    o.id,
    o.channel,
    o.status,
    o.amount
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND YEAR(s.shipped_date) = 2025)
ORDER BY amount DESC, o.status DESC

SELECT
    s.id,
    s.delivered_date,
    s.carrier,
    s.order_id,
    o.discount,
    CONCAT(o.channel, ' ', o.status) AS channel_status
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE s.cost > 33 OR o.channel IN ('mobile', 'store', 'partner')
ORDER BY carrier DESC, delivered_date;

SELECT
    s.order_id,
    s.delivered_date,
    o.channel,
    o.customer_id,
    c.country
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
JOIN customers c ON o.customer_id = c.id
WHERE c.country = 'SG'

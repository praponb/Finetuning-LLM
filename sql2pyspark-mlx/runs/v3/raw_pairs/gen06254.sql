SELECT
    o.id,
    o.customer_id,
    o.order_date,
    o.channel,
    s.store_name
FROM orders o
JOIN stores s ON o.store_id = s.id
WHERE o.order_date < '2024-05-01'
ORDER BY channel, id
LIMIT 20

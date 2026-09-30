SELECT
    oi.id,
    oi.order_id,
    o.customer_id,
    o.channel
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.order_date BETWEEN '2024-05-01' AND '2025-07-23' OR o.channel NOT IN ('partner', 'mobile', 'store')

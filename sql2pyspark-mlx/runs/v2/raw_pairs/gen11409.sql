SELECT
    o.channel,
    o.customer_id,
    o.discount,
    s.cost
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
WHERE s.order_id IS NOT NULL
ORDER BY customer_id DESC

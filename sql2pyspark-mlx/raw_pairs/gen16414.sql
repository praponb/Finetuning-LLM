SELECT
    channel,
    MAX(discount) - MIN(discount) discount_range,
    MAX(order_date) last_order_date,
    MAX(order_date) latest_order_date
FROM orders
WHERE status = 'RETURNED' AND store_id IS NOT NULL
GROUP BY channel
HAVING discount_range >= 78
ORDER BY last_order_date ASC

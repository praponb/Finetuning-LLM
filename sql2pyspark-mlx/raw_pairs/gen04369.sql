SELECT
    status,
    MAX(order_date) AS last_order_date,
    COUNT(discount) AS discount_count,
    MAX(amount) - MIN(amount) AS amount_range
FROM orders
GROUP BY status;

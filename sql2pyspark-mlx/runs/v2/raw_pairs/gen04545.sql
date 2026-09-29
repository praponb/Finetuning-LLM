SELECT
    o.customer_id,
    o.store_id,
    t.stddev_amount,
    t.total_amount
FROM orders o
LEFT OUTER JOIN (SELECT order_id, ROUND(STDDEV(amount), 2) AS stddev_amount, SUM(amount) AS total_amount
    FROM payments
    WHERE status IN ('FAILED', 'SUCCESS', 'REFUNDED')
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.total_amount > 5000
ORDER BY stddev_amount

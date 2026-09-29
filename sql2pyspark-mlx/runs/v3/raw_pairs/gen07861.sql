SELECT
    id,
    method,
    amount,
    order_id,
    AVG(amount) OVER (PARTITION BY method) AS method_avg_amount,
    RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS rnk
FROM payments
ORDER BY 4 DESC

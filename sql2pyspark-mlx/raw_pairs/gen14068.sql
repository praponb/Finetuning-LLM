SELECT
    id,
    order_id,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 3) AS amount_share
FROM payments
WHERE amount BETWEEN 450 AND 1170
ORDER BY amount_share DESC

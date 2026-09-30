SELECT
    id,
    customer_id,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 3) AS amount_share
FROM orders;

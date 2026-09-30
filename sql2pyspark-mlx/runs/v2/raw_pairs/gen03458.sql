SELECT
    id,
    status,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share
FROM orders
WHERE order_date >= '2025-06-06';

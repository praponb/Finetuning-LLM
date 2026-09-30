SELECT
    id,
    status,
    amount,
    RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS rnk
FROM orders
ORDER BY status

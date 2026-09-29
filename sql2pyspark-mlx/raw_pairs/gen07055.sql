SELECT channel, avg_amount
FROM (
    SELECT channel, AVG(amount) AS avg_amount
    FROM orders
    WHERE order_date <= '2025-09-01'
    GROUP BY channel
) AS t
WHERE avg_amount > 900

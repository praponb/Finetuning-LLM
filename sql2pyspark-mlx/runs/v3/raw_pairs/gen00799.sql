SELECT *
FROM (
    SELECT channel, COUNT(amount) AS amount_count
    FROM orders
    WHERE status = 'CANCELLED'
    GROUP BY channel
) AS t
WHERE amount_count >= 4

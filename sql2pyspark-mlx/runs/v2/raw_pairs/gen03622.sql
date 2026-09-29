SELECT *
FROM (
    SELECT status, channel, COUNT(amount) AS amount_count
    FROM orders
    GROUP BY status, channel
) AS agg
WHERE amount_count < 8
ORDER BY status

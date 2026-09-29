SELECT *
FROM (
    SELECT channel, customer_id, COUNT(amount) AS amount_count
    FROM orders
    GROUP BY channel, customer_id
) AS t
WHERE amount_count < 7

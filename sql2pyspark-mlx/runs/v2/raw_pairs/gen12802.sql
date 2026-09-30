SELECT *
FROM (
    SELECT customer_id, channel, SUM(amount) AS total_amount
    FROM orders
    GROUP BY customer_id, channel
) AS t
WHERE total_amount > 4000
ORDER BY customer_id;

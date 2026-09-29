SELECT channel, avg_discount
FROM (
    SELECT channel, ROUND(AVG(discount), 2) AS avg_discount
    FROM orders
    WHERE store_id = 35
    GROUP BY channel
) AS sub
WHERE avg_discount > 50
ORDER BY channel DESC
LIMIT 5;

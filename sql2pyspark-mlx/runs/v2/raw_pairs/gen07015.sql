SELECT *
FROM (
    SELECT status, ROUND(AVG(discount), 2) AS avg_discount
    FROM orders
    GROUP BY status
) AS sub
WHERE avg_discount > 29
ORDER BY status
LIMIT 1

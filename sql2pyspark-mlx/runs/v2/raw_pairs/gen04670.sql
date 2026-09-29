WITH sessions_by_customer AS (
    SELECT customer_id, COUNT(DISTINCT browser) unique_browsers
    FROM sessions
    WHERE browser IN ('Firefox', 'Edge', 'Chrome')
    GROUP BY customer_id
)
SELECT c.id, c.city, sbc.unique_browsers
FROM customers c
INNER JOIN sessions_by_customer sbc ON c.id = sbc.customer_id
WHERE sbc.unique_browsers > 9
ORDER BY unique_browsers DESC

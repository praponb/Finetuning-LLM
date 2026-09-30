WITH customer_stats AS (
    SELECT customer_id, COUNT(DISTINCT monthly_fee) AS unique_plans
    FROM subscriptions
    WHERE YEAR(end_date) = 2024
    GROUP BY customer_id
)
SELECT c.id, c.city, cs.unique_plans
FROM customers c
JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.unique_plans > 7
ORDER BY city, id
LIMIT 5;

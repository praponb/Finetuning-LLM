WITH customer_stats AS (
    SELECT customer_id, COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count
    FROM sessions
    GROUP BY customer_id
)
SELECT customers.name, customers.signup_date, customer_stats.safari_count
FROM customers
LEFT OUTER JOIN customer_stats ON customers.id = customer_stats.customer_id
WHERE customer_stats.safari_count > 7
ORDER BY name DESC;

WITH customer_summary AS (
    SELECT customer_id, COUNT(*) AS cnt
    FROM sessions
    GROUP BY customer_id
)
SELECT c.signup_date, c.name, cs.cnt
FROM customers c
JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY cs.cnt ASC

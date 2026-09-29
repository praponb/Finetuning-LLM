WITH customer_summary AS (
    SELECT customer_id, COUNT(*) AS cnt
    FROM sessions
    WHERE customer_id = 40
    GROUP BY customer_id
)
SELECT c.country, cs.cnt
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
ORDER BY cnt DESC;

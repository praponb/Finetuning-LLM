WITH customer_summary AS (
    SELECT customer_id, SUM(CASE WHEN device = 'desktop' THEN 1 ELSE 0 END) desktop_count
    FROM sessions
    GROUP BY customer_id
)
SELECT c.name, cs.desktop_count
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.desktop_count > 51

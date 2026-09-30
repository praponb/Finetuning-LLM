WITH customer_summary AS (
    SELECT customer_id, SUM(CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END) AS firefox_count, MIN(started_at) AS min_started_at
    FROM sessions
    GROUP BY customer_id
)
SELECT c.name, cs.firefox_count, cs.min_started_at
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.firefox_count >= 59
ORDER BY name DESC, firefox_count DESC

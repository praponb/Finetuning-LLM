WITH customer_summary AS (
    SELECT customer_id, MAX(end_date) AS latest_end_date, SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.city,
    c.age,
    cs.latest_end_date,
    cs.standard_count
FROM customers c
LEFT OUTER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.standard_count >= 73
ORDER BY latest_end_date

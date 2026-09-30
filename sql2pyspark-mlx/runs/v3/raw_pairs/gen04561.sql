WITH sessions_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN device = 'desktop' THEN 1 ELSE 0 END) AS desktop_count, MIN(started_at) AS first_started_at
    FROM sessions
    GROUP BY customer_id
)
SELECT
    customers.is_active,
    customers.email,
    sessions_by_customer.desktop_count,
    sessions_by_customer.first_started_at
FROM customers
JOIN sessions_by_customer ON customers.id = sessions_by_customer.customer_id
WHERE sessions_by_customer.desktop_count > 43
ORDER BY customers.email DESC

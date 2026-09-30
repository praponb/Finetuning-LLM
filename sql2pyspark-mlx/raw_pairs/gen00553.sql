WITH customer_stats AS (
    SELECT customer_id, MAX(started_at) AS latest_started_at
    FROM sessions
    WHERE country IS NOT NULL
    GROUP BY customer_id
)
SELECT customers.name, customer_stats.latest_started_at
FROM customers
JOIN customer_stats ON customers.id = customer_stats.customer_id

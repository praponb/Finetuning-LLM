WITH customer_stats AS (
    SELECT customer_id, MAX(started_at) AS last_started_at, COUNT(*) AS row_count
    FROM sessions
    WHERE country NOT IN ('JP', 'US')
    GROUP BY customer_id
)
SELECT customers.name, customer_stats.last_started_at, customer_stats.row_count
FROM customers
INNER JOIN customer_stats ON customers.id = customer_stats.customer_id

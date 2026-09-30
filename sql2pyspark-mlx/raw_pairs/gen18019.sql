WITH customer_stats AS (
    SELECT customer_id, COUNT(DISTINCT monthly_fee) AS unique_customers, COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count
    FROM subscriptions
    WHERE start_date BETWEEN '2024-07-01' AND '2024-09-01'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.city,
    c.credit_limit,
    cs.unique_customers,
    cs.active_count
FROM customers c
JOIN customer_stats cs ON c.id = cs.customer_id

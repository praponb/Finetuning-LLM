WITH customer_summary AS (
    SELECT customer_id, COUNT(DISTINCT monthly_fee) AS unique_plans, SUM(monthly_fee) AS total_monthly_fee
    FROM subscriptions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    c.id,
    c.name,
    c.age,
    cs.unique_plans,
    cs.total_monthly_fee
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.total_monthly_fee > 1200;

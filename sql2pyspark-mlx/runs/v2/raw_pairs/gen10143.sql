WITH customer_stats AS (
    SELECT customer_id, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.name,
    c.country,
    cs.avg_monthly_fee
FROM customers c
LEFT OUTER JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.avg_monthly_fee > 67
ORDER BY cs.avg_monthly_fee DESC;

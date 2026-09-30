SELECT *
FROM (
    SELECT customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, AVG(monthly_fee) AS avg_monthly_fee
    FROM subscriptions
    WHERE plan = 'enterprise'
    GROUP BY customer_id
) AS t
WHERE stddev_monthly_fee >= 28
ORDER BY customer_id
LIMIT 5;

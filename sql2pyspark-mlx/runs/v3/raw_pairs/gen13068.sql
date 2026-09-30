SELECT customer_id, total_monthly_fee
FROM (
    SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee
    FROM subscriptions
    WHERE plan IN ('standard', 'premium', 'enterprise')
    GROUP BY customer_id
) AS sub
WHERE total_monthly_fee < 1420

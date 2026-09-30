SELECT customer_id, subscription_count, max_monthly_fee
FROM (
    SELECT customer_id, COUNT(*) AS subscription_count, MAX(monthly_fee) AS max_monthly_fee
    FROM subscriptions
    WHERE status = 'CANCELLED'
    GROUP BY customer_id
) AS t
WHERE subscription_count < 6
ORDER BY max_monthly_fee DESC;

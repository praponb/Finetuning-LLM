SELECT plan, COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
GROUP BY 1

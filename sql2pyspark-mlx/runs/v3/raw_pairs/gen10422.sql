SELECT status, COUNT(monthly_fee) AS monthly_fee_count, MIN(start_date) AS first_start_date
FROM subscriptions
GROUP BY status

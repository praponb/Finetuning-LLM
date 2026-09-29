SELECT status, MAX(start_date) AS last_start_date, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
GROUP BY status;

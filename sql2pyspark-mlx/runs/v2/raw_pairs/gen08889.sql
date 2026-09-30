SELECT start_date, ROUND(monthly_fee * 1.1, 2) AS adjusted_monthly_fee
FROM subscriptions;

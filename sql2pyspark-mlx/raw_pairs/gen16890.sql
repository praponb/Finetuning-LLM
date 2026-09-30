SELECT status, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
GROUP BY 1;

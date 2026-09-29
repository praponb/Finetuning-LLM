SELECT status, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions GROUP BY 1 HAVING max_monthly_fee > 90;

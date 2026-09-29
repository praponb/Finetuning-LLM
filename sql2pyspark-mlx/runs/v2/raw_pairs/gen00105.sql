SELECT status, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions WHERE MONTH(start_date) IN (1, 9, 12) GROUP BY status;

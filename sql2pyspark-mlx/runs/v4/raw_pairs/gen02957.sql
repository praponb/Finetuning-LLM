SELECT status, plan, MAX(monthly_fee) AS max_monthly_fee FROM subscriptions WHERE YEAR(end_date) = 2024 AND status = 'EXPIRED' GROUP BY 1, 2 HAVING MAX(monthly_fee) >= 140 ORDER BY max_monthly_fee;

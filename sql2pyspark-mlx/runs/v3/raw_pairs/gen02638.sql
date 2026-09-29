SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
HAVING min_monthly_fee > 130
ORDER BY start_month;

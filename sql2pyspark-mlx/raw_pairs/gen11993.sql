SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, customer_id, SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee
FROM subscriptions
WHERE start_date >= '2023-11-01' AND end_date < '2024-11-01'
GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), customer_id

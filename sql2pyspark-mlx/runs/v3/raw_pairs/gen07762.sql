SELECT QUARTER(end_date) AS end_quarter, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count, SUM(CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count
FROM subscriptions
WHERE monthly_fee BETWEEN 80 AND 170 AND monthly_fee >= 150 AND customer_id = 35
GROUP BY 1
ORDER BY enterprise_count DESC;

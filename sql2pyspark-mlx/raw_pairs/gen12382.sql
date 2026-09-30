SELECT DATE_FORMAT(end_date, 'yyyy-MM') end_month, COUNT(*) subscription_count
FROM subscriptions
WHERE monthly_fee > 210 AND start_date >= '2024-01-01'
GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
HAVING COUNT(*) > 3
ORDER BY 1 DESC;

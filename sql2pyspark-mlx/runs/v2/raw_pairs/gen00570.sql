SELECT c.country, SUM(s.monthly_fee) AS total_monthly_fee, ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.country
ORDER BY total_monthly_fee DESC;

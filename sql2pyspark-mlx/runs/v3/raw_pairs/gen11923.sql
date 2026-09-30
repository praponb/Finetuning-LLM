SELECT c.segment, SUM(s.monthly_fee) AS total_monthly_fee, ROUND(AVG(s.monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions s
JOIN customers c ON c.id = s.customer_id
WHERE s.monthly_fee IS NULL
GROUP BY c.segment
HAVING ROUND(AVG(s.monthly_fee), 2) >= 71
ORDER BY segment ASC

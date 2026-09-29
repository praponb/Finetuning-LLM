SELECT c.country, SUM(CASE WHEN s.status = 'EXPIRED' THEN s.monthly_fee ELSE 0 END) expired_monthly_fee, MAX(s.monthly_fee) max_monthly_fee
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id = 11
GROUP BY c.country
ORDER BY expired_monthly_fee, country
LIMIT 20

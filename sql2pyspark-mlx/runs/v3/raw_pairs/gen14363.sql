SELECT c.country, c.segment, MIN(s.monthly_fee) AS min_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit BETWEEN 7800 AND 10800
GROUP BY c.country, c.segment
ORDER BY country ASC, min_monthly_fee DESC

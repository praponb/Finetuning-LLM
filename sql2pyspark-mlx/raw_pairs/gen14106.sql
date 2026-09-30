SELECT is_active, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE country IN ('SG', 'UK')
GROUP BY is_active
ORDER BY tokyo_count DESC

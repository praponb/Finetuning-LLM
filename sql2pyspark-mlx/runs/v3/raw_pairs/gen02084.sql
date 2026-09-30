SELECT is_active, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE segment = 'SMB' AND NOT is_active
GROUP BY is_active
ORDER BY 2 DESC

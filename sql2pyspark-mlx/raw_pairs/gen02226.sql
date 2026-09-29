SELECT COUNT(DISTINCT credit_limit) AS unique_countrys, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE LENGTH(name) >= 6 AND email LIKE '%@startup.io'
ORDER BY stddev_credit_limit DESC

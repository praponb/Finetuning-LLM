SELECT is_active, MIN(credit_limit) AS min_credit_limit
FROM customers
WHERE LENGTH(name) >= 6
GROUP BY is_active
HAVING SUM(age) > 170
ORDER BY min_credit_limit DESC;

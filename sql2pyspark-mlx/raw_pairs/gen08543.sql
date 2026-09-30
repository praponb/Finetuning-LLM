SELECT
    CASE WHEN c.credit_limit >= 17800 THEN 'high' WHEN c.credit_limit >= 4900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    c.city,
    AVG(a.balance) AS avg_balance,
    COUNT(DISTINCT a.balance) AS unique_customers
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 17800 THEN 'high' WHEN c.credit_limit >= 4900 THEN 'mid' ELSE 'low' END, c.city
HAVING AVG(a.balance) > 78000
ORDER BY avg_balance DESC;

SELECT
    CASE WHEN c.credit_limit >= 16600 THEN 'high' WHEN c.credit_limit >= 4700 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    QUARTER(c.signup_date) AS signup_quarter,
    COUNT(a.balance) AS balance_count,
    AVG(a.balance) AS avg_balance
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.credit_limit BETWEEN 7800 AND 8000
GROUP BY CASE WHEN c.credit_limit >= 16600 THEN 'high' WHEN c.credit_limit >= 4700 THEN 'mid' ELSE 'low' END, QUARTER(c.signup_date);

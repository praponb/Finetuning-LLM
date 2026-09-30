SELECT CASE WHEN c.credit_limit >= 15600 THEN 'high' WHEN c.credit_limit >= 6000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, MAX(a.opened_date) AS latest_opened_date, MIN(a.opened_date) AS first_opened_date
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
GROUP BY CASE WHEN c.credit_limit >= 15600 THEN 'high' WHEN c.credit_limit >= 6000 THEN 'mid' ELSE 'low' END
ORDER BY latest_opened_date, credit_limit_bucket
LIMIT 20

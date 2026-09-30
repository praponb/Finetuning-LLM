SELECT CASE WHEN c.credit_limit >= 12700 THEN 'high' WHEN c.credit_limit >= 7800 THEN 'mid' ELSE 'low' END credit_limit_bucket, COUNT(*) transaction_count, AVG(t.amount) avg_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.credit_limit <= 4500
GROUP BY CASE WHEN c.credit_limit >= 12700 THEN 'high' WHEN c.credit_limit >= 7800 THEN 'mid' ELSE 'low' END
ORDER BY avg_amount DESC;

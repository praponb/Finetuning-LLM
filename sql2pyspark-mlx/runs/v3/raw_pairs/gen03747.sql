SELECT CASE WHEN a.balance >= 66000 THEN 'high' WHEN a.balance >= 31000 THEN 'mid' ELSE 'low' END AS balance_bucket, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
LEFT OUTER JOIN accounts a ON a.id = t.account_id
GROUP BY CASE WHEN a.balance >= 66000 THEN 'high' WHEN a.balance >= 31000 THEN 'mid' ELSE 'low' END
HAVING MAX(t.amount) - MIN(t.amount) >= 42
ORDER BY amount_range

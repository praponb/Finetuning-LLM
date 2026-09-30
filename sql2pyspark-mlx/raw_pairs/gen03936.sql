SELECT a.status, COUNT(DISTINCT t.amount) unique_accounts
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
WHERE a.account_type IN ('CREDIT', 'LOAN')
GROUP BY a.status
HAVING unique_accounts > 5
ORDER BY 1 DESC, unique_accounts DESC
LIMIT 1

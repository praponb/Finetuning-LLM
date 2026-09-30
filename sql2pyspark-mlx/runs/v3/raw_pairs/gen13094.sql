SELECT status, SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance
FROM accounts
WHERE balance BETWEEN 40000 AND 51000 OR status = 'CLOSED'
GROUP BY status
HAVING closed_balance >= 68;

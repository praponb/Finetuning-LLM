SELECT account_type, COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count
FROM accounts
WHERE status = 'OPEN' AND MONTH(opened_date) IN (5, 6, 7) AND account_type IN ('CREDIT', 'LOAN', 'SAVINGS')
GROUP BY account_type
HAVING COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) >= 2
ORDER BY account_type

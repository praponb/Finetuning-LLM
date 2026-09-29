SELECT status, COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
FROM accounts
WHERE balance BETWEEN 51000 AND 54000
GROUP BY status

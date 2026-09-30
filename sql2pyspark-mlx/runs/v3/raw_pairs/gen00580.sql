SELECT status, COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count, MAX(opened_date) AS latest_opened_date
FROM accounts
GROUP BY status
HAVING COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) < 5
ORDER BY status

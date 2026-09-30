SELECT status, MAX(balance) AS max_balance, MIN(opened_date) AS first_opened_date
FROM accounts
WHERE status = 'OPEN' AND (status IN ('OPEN', 'FROZEN', 'CLOSED') OR status NOT IN ('FROZEN', 'CLOSED'))
GROUP BY status
ORDER BY status ASC, first_opened_date ASC

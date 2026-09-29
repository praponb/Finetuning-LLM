SELECT *, DATE_ADD(opened_date, 67) AS opened_date_plus_67d
FROM accounts
WHERE status IN ('CLOSED', 'OPEN', 'FROZEN')
ORDER BY balance DESC

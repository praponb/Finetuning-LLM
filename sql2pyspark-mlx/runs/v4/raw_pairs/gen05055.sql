SELECT *
FROM accounts
WHERE YEAR(opened_date) = 2023 AND status = 'OPEN'

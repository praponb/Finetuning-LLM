SELECT *
FROM accounts
WHERE account_type IN ('CHECKING', 'LOAN', 'CREDIT') AND (status = 'OPEN' OR status = 'OPEN')
ORDER BY opened_date DESC;

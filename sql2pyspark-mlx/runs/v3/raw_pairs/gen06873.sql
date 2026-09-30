SELECT id, opened_date, account_type
FROM accounts
WHERE opened_date <= (SELECT MIN(opened_date) FROM accounts WHERE balance > 20000)

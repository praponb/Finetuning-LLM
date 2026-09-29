SELECT id, opened_date, balance
FROM accounts
WHERE opened_date < (SELECT MIN(opened_date) FROM accounts)
ORDER BY opened_date, id DESC;

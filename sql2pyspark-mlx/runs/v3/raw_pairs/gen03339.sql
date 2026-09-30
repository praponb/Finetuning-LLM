SELECT
    id,
    opened_date,
    status,
    balance
FROM accounts
WHERE opened_date = (SELECT MIN(opened_date) FROM accounts)
ORDER BY id DESC;

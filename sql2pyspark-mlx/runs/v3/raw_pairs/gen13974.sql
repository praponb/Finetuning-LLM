SELECT
    id,
    balance,
    customer_id,
    opened_date
FROM accounts
WHERE balance < (SELECT MIN(balance) FROM accounts);

SELECT
    transactions.id,
    transactions.account_id,
    accounts.account_type,
    accounts.opened_date,
    accounts.customer_id
FROM transactions
LEFT JOIN accounts ON transactions.account_id = accounts.id;

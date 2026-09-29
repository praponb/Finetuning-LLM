SELECT
    transactions.id,
    transactions.txn_time,
    accounts.account_type,
    accounts.balance,
    customers.is_active
FROM transactions
JOIN accounts ON accounts.id = transactions.account_id
JOIN customers ON accounts.customer_id = customers.id
ORDER BY balance DESC, transactions.id DESC;

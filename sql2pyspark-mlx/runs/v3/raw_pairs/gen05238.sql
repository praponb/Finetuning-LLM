SELECT
    accounts.customer_id,
    transactions.is_flagged,
    transactions.amount,
    transactions.txn_type
FROM accounts
LEFT OUTER JOIN transactions ON accounts.id = transactions.account_id
WHERE transactions.amount BETWEEN 2700 AND 3100;

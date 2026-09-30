SELECT
    transactions.id,
    transactions.amount,
    transactions.account_id,
    transactions.merchant,
    accounts.account_type,
    accounts.customer_id,
    accounts.balance
FROM transactions
LEFT JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.is_flagged = TRUE OR accounts.account_type IN ('CHECKING', 'LOAN', 'CREDIT')
ORDER BY accounts.balance DESC;

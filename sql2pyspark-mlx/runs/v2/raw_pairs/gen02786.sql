SELECT
    accounts.id,
    accounts.status,
    COUNT(transactions.id) AS transaction_count,
    SUM(transactions.amount) AS total_amount,
    COUNT(DISTINCT transactions.amount) AS unique_merchants
FROM accounts
LEFT OUTER JOIN transactions ON accounts.id = transactions.account_id
WHERE accounts.balance > 40000
GROUP BY accounts.id, accounts.status
HAVING transaction_count >= 4;

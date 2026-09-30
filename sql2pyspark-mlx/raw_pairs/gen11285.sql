SELECT
    accounts.account_type,
    COUNT(transactions.id) transaction_count,
    COUNT(CASE WHEN transactions.merchant = '7-Eleven' THEN 1 END) 7_eleven_count,
    SUM(CASE WHEN transactions.txn_type = 'CREDIT' THEN transactions.amount ELSE 0 END) credit_amount
FROM accounts
LEFT JOIN transactions ON accounts.id = transactions.account_id
GROUP BY accounts.account_type

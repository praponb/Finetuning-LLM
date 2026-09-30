WITH transactions_by_account AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count, COUNT(DISTINCT amount) AS unique_accounts
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.account_type, transactions_by_account.starbucks_count, transactions_by_account.unique_accounts
FROM accounts
JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
ORDER BY transactions_by_account.starbucks_count DESC;

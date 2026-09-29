WITH transactions_by_account AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) starbucks_amount, COUNT(DISTINCT amount) unique_accounts
    FROM transactions
    WHERE is_flagged = TRUE
    GROUP BY account_id
)
SELECT accounts.account_type, transactions_by_account.starbucks_amount, transactions_by_account.unique_accounts
FROM accounts
LEFT JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
WHERE transactions_by_account.unique_accounts > 7
ORDER BY account_type DESC

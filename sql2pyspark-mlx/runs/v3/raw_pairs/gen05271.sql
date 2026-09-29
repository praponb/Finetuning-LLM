WITH account_summary AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Amazon' THEN amount ELSE 0 END) AS amazon_amount, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    WHERE txn_type = 'CREDIT'
    GROUP BY account_id
)
SELECT
    accounts.customer_id,
    accounts.status,
    account_summary.amazon_amount,
    account_summary.unique_txn_types
FROM accounts
LEFT OUTER JOIN account_summary ON accounts.id = account_summary.account_id
ORDER BY 3 ASC

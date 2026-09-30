WITH transactions_by_account AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_txn_types
    FROM transactions
    WHERE account_id = 9
    GROUP BY account_id
)
SELECT a.id, a.balance, tba.unique_txn_types
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id

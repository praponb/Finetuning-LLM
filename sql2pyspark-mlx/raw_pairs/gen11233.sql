WITH account_totals AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, MAX(txn_time) AS latest_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    at.amount_range,
    at.latest_txn_time
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
ORDER BY a.id DESC, latest_txn_time
LIMIT 1

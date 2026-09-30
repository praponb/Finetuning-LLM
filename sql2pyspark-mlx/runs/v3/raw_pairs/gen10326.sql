WITH account_totals AS (
    SELECT account_id, COUNT(amount) amount_count, MAX(txn_time) latest_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    at.amount_count,
    at.latest_txn_time
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id

WITH account_totals AS (
    SELECT account_id, MIN(txn_time) AS first_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.opened_date, at.first_txn_time
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id;

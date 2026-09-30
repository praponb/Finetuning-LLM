WITH account_totals AS (
    SELECT account_id, COUNT(*) AS cnt
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2023-10-01'
    GROUP BY account_id
)
SELECT a.account_type, at.cnt
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.cnt > 3;

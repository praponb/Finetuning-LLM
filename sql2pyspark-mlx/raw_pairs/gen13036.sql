WITH account_totals AS (
    SELECT account_id, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, MAX(txn_time) AS last_txn_time
    FROM transactions
    WHERE amount > 3700
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    at.merchant_list,
    at.last_txn_time
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
ORDER BY last_txn_time;

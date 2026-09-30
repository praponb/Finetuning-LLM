WITH account_stats AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values
    FROM transactions
    WHERE txn_type <> 'FEE'
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.status,
    as.merchant_values
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id
ORDER BY balance

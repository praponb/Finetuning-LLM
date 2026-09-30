WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE customer_id = 6 AND account_type = 'CHECKING'
),
agg AS (
    SELECT
        CASE WHEN balance >= 87000 THEN 'high' WHEN balance >= 16000 THEN 'mid' ELSE 'low' END AS balance_bucket,
        account_type,
        MAX(balance) AS max_balance,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM recent_accounts
    GROUP BY CASE WHEN balance >= 87000 THEN 'high' WHEN balance >= 16000 THEN 'mid' ELSE 'low' END, account_type
)
SELECT
    balance_bucket,
    account_type,
    max_balance,
    status_values
FROM agg
WHERE max_balance >= 55000
ORDER BY account_type DESC, max_balance DESC;

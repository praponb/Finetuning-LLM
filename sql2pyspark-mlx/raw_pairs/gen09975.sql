WITH AccountSummary AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count
    FROM transactions
    WHERE is_flagged = TRUE
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.status,
    A.merchant_values,
    A.shell_count
FROM accounts a
LEFT OUTER JOIN AccountSummary A ON a.id = A.account_id
WHERE A.shell_count > 77
ORDER BY balance DESC;

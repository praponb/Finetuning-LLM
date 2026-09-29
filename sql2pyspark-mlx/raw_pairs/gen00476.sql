WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    STDDEV_SAMP(balance) AS balance_stddev,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
FROM kept
GROUP BY DATE_TRUNC('QUARTER', opened_date)
HAVING STDDEV_SAMP(balance) >= 0
ORDER BY quarter_start NULLS LAST

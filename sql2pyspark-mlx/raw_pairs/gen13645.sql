WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type = 'DEBIT'
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        status,
        AVG(balance) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) balance_avg_5,
        STDDEV_SAMP(balance) OVER (PARTITION BY account_type) account_type_stddev_samp_balance
    FROM eligible
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant IN ('Shell', 'Lazada')
        )
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(account_type_stddev_samp_balance) IGNORE NULLS OVER (PARTITION BY status ORDER BY account_type_stddev_samp_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_account_type_stddev_samp_balance,
        CUME_DIST() OVER (PARTITION BY account_type, status ORDER BY account_type_stddev_samp_balance ASC NULLS LAST) account_type_stddev_samp_balance_cume_dist
    FROM kept
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY account_type_stddev_samp_balance ASC NULLS LAST, id ASC) AS rank_in_group
    FROM with_windows_accounts
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        status,
        balance_avg_5,
        account_type_stddev_samp_balance,
        last_known_account_type_stddev_samp_balance,
        LAST_VALUE(account_type_stddev_samp_balance_cume_dist, TRUE) OVER w_grp last_known_account_type_stddev_samp_balance_cume_dist,
        SUM(account_type_stddev_samp_balance_cume_dist) OVER w_hist cumulative_account_type_stddev_samp_balance_cume_dist
    FROM ranked
    WHERE status = 'OPEN'
    WINDOW w_grp AS (PARTITION BY status ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY customer_id ORDER BY balance_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_id,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(DISTINCT status) AS distinct_status,
        ROUND(SUM(account_type_stddev_samp_balance) / NULLIF(COUNT(CASE WHEN status = 'FROZEN' THEN 1 END), 0), 2) AS account_type_stddev_samp_balance_per_frozen,
        SIZE(COLLECT_SET(status)) AS num_status_2,
        SUM(balance_avg_5) AS total_balance_avg_5,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids
    FROM metrics
    GROUP BY customer_id
)
SELECT
    customer_id,
    num_status,
    distinct_status,
    account_type_stddev_samp_balance_per_frozen,
    num_status_2,
    total_balance_avg_5,
    distinct_customer_ids
FROM grouped
ORDER BY num_status ASC

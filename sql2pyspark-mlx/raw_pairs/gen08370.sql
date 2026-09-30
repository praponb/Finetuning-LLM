WITH valid AS (
    SELECT
        *,
        ROUND(balance * 0.85, 2) AS balance_scaled,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE status IS NOT NULL
      AND account_type IN ('LOAN', 'SAVINGS')
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount BETWEEN 1800 AND 3000
        )
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS FIRST, id) AS row_num
        FROM eligible
    ) x
    WHERE row_num = 1
),
metrics AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        resolved_status,
        row_num,
        MIN(balance_scaled) OVER w_ordered AS running_min_balance_scaled,
        SUM(balance_scaled) OVER w_ordered AS running_sum_balance_scaled,
        AVG(balance_scaled) OVER w_ordered AS running_avg_balance_scaled,
        MAX(balance_scaled) OVER w_ordered AS running_max_balance_scaled
    FROM latest
    WINDOW w_ordered AS (PARTITION BY account_type ORDER BY balance_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible_accounts AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type IN ('CREDIT', 'FEE')
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible_accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS running_min_balance_scaled_dense_rank,
        SUM(running_sum_balance_scaled) OVER w_recent AS cumulative_running_sum_balance_scaled
    FROM not_flagged
    WINDOW w_grp AS (ORDER BY running_min_balance_scaled NULLS LAST), w_recent AS (PARTITION BY customer_id ORDER BY running_min_balance_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    customer_id,
    COUNT_IF(status = 'OPEN') AS open_events,
    COUNT_IF(status = 'FROZEN') AS frozen_events,
    APPROX_PERCENTILE(running_max_balance_scaled, ARRAY(0.25, 0.5, 0.75)) AS running_max_balance_scaled_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status,
    APPROX_PERCENTILE(running_max_balance_scaled, ARRAY(0.25, 0.5, 0.75)) AS running_max_balance_scaled_quartiles_2,
    SIZE(COLLECT_SET(status)) AS num_status_2
FROM scored
GROUP BY DATE_TRUNC('QUARTER', opened_date), customer_id;

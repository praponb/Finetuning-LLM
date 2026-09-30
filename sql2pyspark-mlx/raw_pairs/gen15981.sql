WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS rank_in_group
        FROM accounts
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        balance,
        status,
        rank_in_group,
        SUM(balance) OVER (PARTITION BY status, customer_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_balance,
        MAX(balance) OVER (PARTITION BY status, customer_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_balance,
        MIN(balance) OVER (PARTITION BY status, customer_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_balance
    FROM deduped
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_sum_balance DESC NULLS FIRST, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
deduped_accounts AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        rank_in_group,
        running_max_balance,
        row_num
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY running_min_balance DESC, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_max_balance >= 65000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_balance_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(running_max_balance), 2) AS avg_running_max_balance,
    PERCENTILE_APPROX(running_max_balance, ARRAY(0.25, 0.5, 0.75)) AS running_max_balance_quartiles,
    PERCENTILE_APPROX(running_max_balance, 0.5) AS running_max_balance_p50
FROM deduped_accounts
GROUP BY CASE WHEN running_max_balance >= 65000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING running_max_balance_p50 > 0
ORDER BY avg_running_max_balance NULLS LAST

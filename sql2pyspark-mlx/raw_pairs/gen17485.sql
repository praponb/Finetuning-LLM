WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END is_checking
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        opened_date,
        status,
        MAX(is_checking) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_is_checking,
        AVG(is_checking) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_is_checking,
        MIN(is_checking) OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_is_checking
    FROM valid
    WHERE opened_date >= '2023-08-17'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date NULLS LAST, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY running_avg_is_checking, id) AS row_num_2
    FROM deduped
    QUALIFY row_num_2 = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_avg_is_checking) OVER w_grp prev_running_avg_is_checking,
        LEAD(running_avg_is_checking) OVER w_grp next_running_avg_is_checking,
        LAG(running_min_is_checking) OVER w_grp prev_running_min_is_checking,
        LEAD(running_min_is_checking) OVER w_grp next_running_min_is_checking,
        ROW_NUMBER() OVER w_grp row_seq
    FROM latest
    WINDOW w_grp AS (PARTITION BY status ORDER BY row_num_2, id)
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_avg_is_checking) OVER w_running next_running_avg_is_checking_2,
        LEAD(next_running_min_is_checking) OVER w_running next_next_running_min_is_checking,
        LAG(next_running_min_is_checking) OVER w_running prev_next_running_min_is_checking,
        ROW_NUMBER() OVER w_running row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date), status ORDER BY prev_running_avg_is_checking, id) row_seq_3
    FROM with_windows
    WINDOW w_running AS (PARTITION BY status, account_type ORDER BY opened_date, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        account_type,
        next_running_avg_is_checking_2,
        prev_next_running_min_is_checking,
        running_max_is_checking
    FROM with_windows_accounts
) src
UNPIVOT (
    metric_value FOR metric_name IN (next_running_avg_is_checking_2, prev_next_running_min_is_checking AS PREV_NEXT_RUNNING_MIN_IS_CHECKING_VAL, running_max_is_checking AS RUNNING_MAX_IS_CHECKING)
) u

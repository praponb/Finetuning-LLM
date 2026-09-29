WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DAYOFWEEK(start_date) AS start_date_dayofweek
    FROM policies
    WHERE status = 'EXPIRED'
      AND holder_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(premium, 2) OVER (ORDER BY start_date_dayofweek NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(CUME_DIST() OVER w_all_by_start_date_dayofweek_2, 3) AS start_date_dayofweek_cume_dist,
        FIRST_VALUE(start_date_dayofweek) IGNORE NULLS OVER (ORDER BY start_date_dayofweek NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_dayofweek,
        ROUND(AVG(premium) OVER (ORDER BY start_date_dayofweek NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2
    FROM valid_policys
    WINDOW w_all_by_start_date_dayofweek_2 AS (ORDER BY start_date_dayofweek NULLS LAST)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        start_date_dayofweek,
        second_premium,
        start_date_dayofweek_cume_dist,
        first_known_start_date_dayofweek,
        premium_avg_2,
        ROUND(start_date_dayofweek - AVG(start_date_dayofweek) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date)), 2) AS start_date_dayofweek_vs_holder_id_end_date_month,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date_dayofweek_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        COUNT(*) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date)) AS rows_in_holder_id_end_date_month,
        RANK() OVER (ORDER BY second_premium, id) AS second_premium_rank
    FROM with_windows
)
SELECT id, measure, value
FROM (
    SELECT id, second_premium_rank, rows_in_holder_id_end_date_month, first_known_start_date_dayofweek, start_date_dayofweek
    FROM with_windows_2
) src
UNPIVOT (
    value FOR measure IN (second_premium_rank, rows_in_holder_id_end_date_month, first_known_start_date_dayofweek, start_date_dayofweek)
) unp

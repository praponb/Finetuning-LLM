WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE qty <= 332
      AND MONTH(moved_at) IN (2, 3)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        move_type,
        moved_at,
        operator_group,
        MONTH(moved_at) AS moved_at_month,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        CASE WHEN operator_group IN ('group_a', 'other') THEN 'group_a' WHEN operator_group = 'group_b' THEN 'group_b' ELSE 'other' END AS operator_group_group
    FROM filtered
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        moved_at,
        operator_group,
        moved_at_month,
        move_type_prefix,
        operator_group_group,
        LEAD(moved_at_month, 2) OVER w_moved_at_to_date_by_moved_at AS next2_moved_at_month,
        ROUND(moved_at_month - AVG(moved_at_month) OVER w_moved_at_month_operator_grou, 2) AS moved_at_month_vs_moved_at_month_operator_group_,
        MIN(moved_at_month) OVER (PARTITION BY MONTH(moved_at), operator_group_group) AS moved_at_month_operator_group_group_min_moved_at
    FROM annotated
    WINDOW w_moved_at_to_date_by_moved_at AS (PARTITION BY TO_DATE(moved_at) ORDER BY moved_at_month DESC, id), w_moved_at_month_operator_grou AS (PARTITION BY MONTH(moved_at), operator_group_group)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        ROUND(SUM(moved_at_month_operator_group_group_min_moved_at) / NULLIF(SUM(moved_at_month_vs_moved_at_month_operator_group_), 0), 3) AS moved_at_month_operator_group_group_min_moved_at_2,
        MIN(moved_at) AS first_moved_at
    FROM metrics
    WHERE operator_group NOT IN ('other', 'group_a')
    GROUP BY 1
    HAVING MAX(moved_at_month_vs_moved_at_month_operator_group_) > -8.4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(first_moved_at) ORDER BY moved_at_month_operator_group_group_min_moved_at_2), 3) AS moved_at_month_operator_group_group_min_moved_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_moved_at) ORDER BY first_moved_at), 3) AS first_moved_at_percent_rank,
        RANK() OVER (PARTITION BY MONTH(first_moved_at) ORDER BY moved_at_month_operator_group_group_min_moved_at_2, moved_at_day) AS moved_at_month_operator_group_group_min_moved_at_3
    FROM rollup_base
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(first_moved_at_percent_rank / NULLIF(SUM(first_moved_at_percent_rank) OVER w_moved_at_month_operator_grou_2, 0), 4) AS first_moved_at_percent_rank_share_of_moved_at_mo,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at_month_operator_group_group_min_moved_at DESC), 3) AS moved_at_month_operator_group_group_min_moved_at_4
    FROM with_trends
    WINDOW w_moved_at_month_operator_grou_2 AS (PARTITION BY moved_at_month_operator_group_group_min_moved_at_3)
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics_2
    WHERE first_moved_at_percent_rank_share_of_moved_at_mo > (SELECT MIN(first_moved_at_percent_rank_share_of_moved_at_mo) FROM metrics_2)
)
-- Final projection
SELECT
    moved_at_day,
    moved_at_month_operator_group_group_min_moved_at_2,
    first_moved_at,
    first_moved_at_percent_rank,
    moved_at_month_operator_group_group_min_moved_at_3,
    first_moved_at_percent_rank_share_of_moved_at_mo,
    moved_at_month_operator_group_group_min_moved_at_4,
    MONTH(moved_at_day) AS moved_at_day_month
FROM without_matches
ORDER BY first_moved_at DESC, moved_at_month_operator_group_group_min_moved_at_2, moved_at_day
LIMIT 5

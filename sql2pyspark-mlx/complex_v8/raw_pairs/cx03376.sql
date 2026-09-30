WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
    FROM scoped_moves
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(moved_at) AS moved_at_day,
        YEAR(moved_at_plus_15m) AS moved_at_plus_15m_year,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE operator IN ('op_eve', 'op_ann', 'op_dew')) AS n_operator_in_op_eve_op_ann_op,
        ROUND(AVG(qty), 2) AS avg_qty,
        COUNT(*) FILTER (WHERE operator IN ('op_ann', 'op_dew')) AS n_operator_in_op_ann_op_dew
    FROM scored
    GROUP BY TO_DATE(moved_at), YEAR(moved_at_plus_15m)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY avg_qty DESC, moved_at_day, moved_at_plus_15m_year) AS rn,
        ROUND(CUME_DIST() OVER w_all_by_avg_qty, 3) AS avg_qty_cume_dist,
        FIRST_VALUE(moved_at_plus_15m_year) IGNORE NULLS OVER (ORDER BY avg_qty DESC, moved_at_day, moved_at_plus_15m_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at_plus_15m_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY avg_qty DESC, moved_at_day, moved_at_plus_15m_year), 3) AS avg_qty_percent_rank,
        ROUND(n_operator_in_op_ann_op_dew - AVG(n_operator_in_op_ann_op_dew) OVER w_row_count, 2) AS n_operator_in_op_ann_op_dew_vs_row_count_avg
    FROM summary
    WINDOW w_all_by_avg_qty AS (ORDER BY avg_qty DESC, moved_at_day, moved_at_plus_15m_year), w_row_count AS (PARTITION BY row_count)
),
labeled AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2025-12-31', moved_at_day) AS days_since_moved_at_day
    FROM with_windows
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM labeled
    WHERE rn = 1
      AND n_operator_in_op_eve_op_ann_op >= 2
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM top_rows
    WHERE avg_qty > (SELECT MIN(avg_qty) FROM top_rows)
)
SELECT
    moved_at_day,
    moved_at_plus_15m_year,
    row_count,
    n_operator_in_op_eve_op_ann_op,
    avg_qty,
    avg_qty_cume_dist,
    first_known_moved_at_plus_15m_year,
    avg_qty_percent_rank,
    n_operator_in_op_ann_op_dew_vs_row_count_avg,
    days_since_moved_at_day
FROM filtered

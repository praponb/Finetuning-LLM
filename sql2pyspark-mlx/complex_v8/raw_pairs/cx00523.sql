WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE operator = 'op_eve'
),
per_group AS (
    -- Group-level totals
    SELECT
        operator,
        move_type,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        MIN(moved_at) AS first_moved_at,
        SUM(unit_price) AS total_unit_price,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ROUND(AVG(unit_price), 2) AS avg_unit_price,
        ROUND(AVG(qty), 2) AS avg_qty,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio
    FROM prepared_moves
    WHERE qty BETWEEN 67 AND 314
    GROUP BY 1, 2, 3
    HAVING MAX(qty) > 123
),
scored AS (
    -- Add calculated columns
    SELECT
        operator,
        move_type,
        moved_at_month,
        first_moved_at,
        move_type_list,
        avg_unit_price,
        avg_qty,
        unit_price_to_qty_ratio,
        DATEDIFF(DATE '2025-12-31', first_moved_at) AS days_since_first_moved_at,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        FLOOR(total_unit_price / 500) * 500 AS total_unit_price_bucket
    FROM per_group
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(days_since_first_moved_at) OVER (PARTITION BY operator ORDER BY unit_price_to_qty_ratio DESC, operator, move_type, moved_at_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_first_moved_at_sum_4,
        FIRST_VALUE(total_unit_price_bucket) OVER (PARTITION BY move_type ORDER BY first_moved_at DESC, operator, move_type, moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_unit_price_bucket,
        NTH_VALUE(days_since_first_moved_at, 2) OVER (PARTITION BY operator ORDER BY unit_price_to_qty_ratio DESC, operator, move_type, moved_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_first_moved_at,
        RANK() OVER w_operator_by_unit_price_to_qt_3 AS unit_price_to_qty_ratio_rank,
        ROW_NUMBER() OVER (PARTITION BY operator ORDER BY first_moved_at DESC NULLS FIRST, operator, move_type, moved_at_month) AS first_moved_at_seq,
        LEAD(avg_qty) OVER (PARTITION BY move_type ORDER BY first_moved_at DESC, operator, move_type, moved_at_month) AS next_avg_qty
    FROM scored
    WINDOW w_operator_by_unit_price_to_qt_3 AS (PARTITION BY operator ORDER BY unit_price_to_qty_ratio DESC, operator, move_type, moved_at_month)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE avg_unit_price > (SELECT AVG(avg_unit_price) FROM with_windows)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        operator,
        move_type,
        moved_at_month,
        first_moved_at,
        move_type_list,
        avg_qty,
        move_type_slug,
        total_unit_price_bucket,
        days_since_first_moved_at_sum_4,
        second_days_since_first_moved_at,
        unit_price_to_qty_ratio_rank,
        first_moved_at_seq,
        next_avg_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY unit_price_to_qty_ratio_rank ORDER BY first_total_unit_price_bucket ASC, operator, move_type, moved_at_month), 3) AS first_total_unit_price_bucket_cume_dist,
        DENSE_RANK() OVER w_unit_price_to_qty_ratio_rank AS first_total_unit_price_bucket_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY unit_price_to_qty_ratio_rank ORDER BY first_total_unit_price_bucket ASC, operator, move_type, moved_at_month) AS first_total_unit_price_bucket_seq
    FROM with_activity
    WINDOW w_unit_price_to_qty_ratio_rank AS (PARTITION BY unit_price_to_qty_ratio_rank ORDER BY first_total_unit_price_bucket ASC, operator, move_type, moved_at_month)
),
exploded AS (
    -- One row per move_type_list_item
    SELECT
        s.operator,
        s.move_type,
        s.moved_at_month,
        s.first_moved_at,
        s.avg_qty,
        s.move_type_slug,
        s.total_unit_price_bucket,
        s.days_since_first_moved_at_sum_4,
        s.second_days_since_first_moved_at,
        s.unit_price_to_qty_ratio_rank,
        s.first_moved_at_seq,
        s.next_avg_qty,
        s.first_total_unit_price_bucket_cume_dist,
        s.first_total_unit_price_bucket_dense_rank,
        s.first_total_unit_price_bucket_seq,
        t.move_type_list_item
    FROM with_trends s
    LATERAL VIEW EXPLODE(s.move_type_list) t AS move_type_list_item
)
-- Final projection
SELECT
    operator,
    moved_at_month,
    first_moved_at,
    total_unit_price_bucket,
    days_since_first_moved_at_sum_4,
    first_moved_at_seq,
    next_avg_qty,
    first_total_unit_price_bucket_cume_dist,
    first_total_unit_price_bucket_dense_rank,
    first_total_unit_price_bucket_seq,
    move_type_list_item,
    UPPER(TRIM(move_type_list_item)) AS move_type_list_item_clean,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM exploded
ORDER BY total_unit_price_bucket NULLS LAST, moved_at_month

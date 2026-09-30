WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE qty BETWEEN 137 AND 415
),
joined AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator_slug,
        s.moved_at_label,
        sk.sku_code,
        sk.is_active AS sku_is_active,
        sk.category AS sku_category
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator_slug,
        sku_code,
        sku_is_active,
        sku_category,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        LEAD(moved_at, 3) OVER w_warehouse_id_by_moved_at AS next3_moved_at,
        NTILE(3) OVER (PARTITION BY sku_id ORDER BY moved_at, id) AS moved_at_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC), 3) AS moved_at_percent_rank,
        RANK() OVER (PARTITION BY move_type, sku_id ORDER BY moved_at NULLS LAST) AS moved_at_rank,
        LEAD(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id) AS next_moved_at,
        NTILE(3) OVER (PARTITION BY warehouse_id ORDER BY moved_at DESC, id) AS moved_at_tercile_2
    FROM joined
    WINDOW w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at DESC, id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(minutes_since_prev_moved_at / NULLIF(SUM(minutes_since_prev_moved_at) OVER (PARTITION BY move_type), 0), 4) AS minutes_since_prev_moved_at_share_of_move_type,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at_tercile DESC), 3) AS moved_at_tercile_percent_rank,
        DENSE_RANK() OVER w_warehouse_id_by_moved_at_ter AS moved_at_tercile_2_dense_rank,
        ROW_NUMBER() OVER w_all_by_moved_at_tercile AS rn,
        SIZE(COLLECT_SET(sku_category) OVER (PARTITION BY move_type)) AS distinct_sku_category_in_move_type,
        ROUND(moved_at_tercile / NULLIF(SUM(moved_at_tercile) OVER (PARTITION BY move_type), 0), 4) AS moved_at_tercile_share_of_move_type,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at_tercile DESC), 3) AS moved_at_tercile_cume_dist,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY moved_at_tercile_2 DESC, id) AS moved_at_tercile_2_quintile
    FROM with_trends
    WINDOW w_warehouse_id_by_moved_at_ter AS (PARTITION BY warehouse_id ORDER BY moved_at_tercile_2 DESC), w_all_by_moved_at_tercile AS (ORDER BY moved_at_tercile DESC, id)
),
stats AS (
    -- Group-level totals
    SELECT
        sku_is_active,
        DATE_TRUNC('MONTH', next3_moved_at) AS next3_moved_at_month,
        distinct_sku_category_in_move_type,
        SUM(CASE WHEN move_type <> 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound,
        SUM(CASE WHEN sku_category NOT IN ('Electronics', 'Beauty') THEN 1 ELSE 0 END) AS n_sku_category_not_in_electron,
        ROUND(SUM(minutes_since_prev_moved_at) / NULLIF(SUM(moved_at_percent_rank), 0), 3) AS minutes_since_prev_moved_at_to_moved_at_percent_,
        ROUND(SUM(moved_at_percent_rank), 2) AS total_moved_at_percent_rank
    FROM metrics
    GROUP BY 1, 2, 3
    HAVING SUM(minutes_since_prev_moved_at) > 1750.0
)
SELECT
    sku_is_active,
    distinct_sku_category_in_move_type,
    n_move_type_outbound,
    n_sku_category_not_in_electron,
    minutes_since_prev_moved_at_to_moved_at_percent_,
    total_moved_at_percent_rank
FROM stats
ORDER BY n_sku_category_not_in_electron, total_moved_at_percent_rank

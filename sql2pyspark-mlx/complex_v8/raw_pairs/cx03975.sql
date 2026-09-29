WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE qty <= 355
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY operator, move_type ORDER BY moved_at_month DESC, id) AS moved_at_month_decile,
        NTH_VALUE(unit_price_bucket, 2) OVER (PARTITION BY operator, move_type ORDER BY moved_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_bucket,
        FIRST_VALUE(unit_price_bucket) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_bucket,
        NTILE(3) OVER (PARTITION BY move_type ORDER BY moved_at_month, id) AS moved_at_month_tercile,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER (PARTITION BY warehouse_id, sku_id), 0), 4) AS unit_price_bucket_share_of_warehouse_id_sku_id,
        LAST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, move_type)) AS distinct_operator_in_warehouse_id_move_type,
        RANK() OVER (PARTITION BY move_type ORDER BY moved_at_month) AS moved_at_month_rank,
        LEAD(moved_at_month, 3) OVER (ORDER BY qty DESC, id) AS next3_moved_at_month
    FROM cleaned
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(next3_moved_at_month) AS next3_moved_at_month_year,
        TO_DATE(moved_at_month) AS moved_at_month_day,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        ROUND(AVG(moved_at_month_decile), 2) AS avg_moved_at_month_decile,
        ROUND(SUM(unit_price_bucket) / NULLIF(SUM(unit_price_bucket_share_of_warehouse_id_sku_id), 0), 3) AS unit_price_bucket_to_unit_price_bucket_share_of_,
        ROUND(100 * COUNT(CASE WHEN operator <> 'op_ann' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_ann,
        COUNT(*) AS cnt,
        COUNT(DISTINCT move_type) AS distinct_move_types
    FROM metrics
    WHERE sku_id <= 4
    GROUP BY 1, 2
)
-- Final projection
SELECT
    next3_moved_at_month_year,
    moved_at_month_day,
    distinct_warehouse_ids,
    avg_moved_at_month_decile,
    pct_operator_op_ann,
    distinct_move_types,
    CASE WHEN distinct_warehouse_ids >= 7 THEN 'large' WHEN distinct_warehouse_ids >= 3 THEN 'medium' ELSE 'small' END AS distinct_warehouse_ids_tier
FROM grouped
ORDER BY distinct_move_types, next3_moved_at_month_year

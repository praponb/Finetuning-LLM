WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE warehouse_id <= 4
),
enriched_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.move_type_prefix,
        s.moved_at_hour,
        sk.sku_code
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.sku_code IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type_prefix,
        MIN(qty) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_min_qty,
        COUNT(*) OVER w_sku_id_warehouse_id AS rows_in_sku_id_warehouse_id,
        NTH_VALUE(qty, 2) OVER (PARTITION BY warehouse_id, MONTH(moved_at_hour) ORDER BY moved_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        SIZE(COLLECT_SET(operator) OVER w_sku_id_warehouse_id) AS distinct_operator_in_sku_id_warehouse_id,
        NTILE(3) OVER w_warehouse_id_moved_at_hour_m_2 AS moved_at_hour_tercile,
        ROUND(CUME_DIST() OVER (ORDER BY qty), 3) AS qty_cume_dist,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at_hour DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_3,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at_hour DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        SUM(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY moved_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM enriched_sku
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_warehouse_id_moved_at_hour_m_2 AS (PARTITION BY warehouse_id, MONTH(moved_at_hour) ORDER BY moved_at_hour, id)
),
stats AS (
    -- Summarise per group
    SELECT
        sku_id,
        moved_at_hour_tercile,
        distinct_operator_in_sku_id_warehouse_id,
        ROUND(STDDEV(rows_in_sku_id_warehouse_id), 2) AS stddev_rows_in_sku_id_warehouse_id,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        PERCENTILE_APPROX(second_qty, 0.5) AS median_second_qty,
        COUNT(*) AS row_count
    FROM ranked
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_sku_id_distinct_operator_in AS rows_in_sku_id_distinct_operator_in_sku_id_wareh,
        LAST_VALUE(moved_at_hour_tercile) IGNORE NULLS OVER (PARTITION BY row_count, sku_id ORDER BY row_count DESC, sku_id, moved_at_hour_tercile, distinct_operator_in_sku_id_warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at_hour_tercile,
        NTILE(4) OVER (PARTITION BY row_count, sku_id ORDER BY row_count DESC, sku_id, moved_at_hour_tercile, distinct_operator_in_sku_id_warehouse_id) AS row_count_quartile,
        ROW_NUMBER() OVER w_distinct_operator_in_sku_id AS row_count_seq
    FROM stats
    WINDOW w_sku_id_distinct_operator_in AS (PARTITION BY sku_id, distinct_operator_in_sku_id_warehouse_id), w_distinct_operator_in_sku_id AS (PARTITION BY distinct_operator_in_sku_id_warehouse_id ORDER BY row_count, sku_id, moved_at_hour_tercile, distinct_operator_in_sku_id_warehouse_id)
)
SELECT
    sku_id,
    moved_at_hour_tercile,
    median_second_qty,
    row_count_quartile,
    row_count_seq,
    COALESCE(row_count_quartile, 0) AS row_count_quartile_filled
FROM with_windows
ORDER BY row_count_quartile DESC, sku_id DESC

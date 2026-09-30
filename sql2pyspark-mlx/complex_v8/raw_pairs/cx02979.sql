WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE unit_price < 393.3
      AND qty < 387
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)), 0), 4) AS unit_price_share_of_moved_at_month,
        FIRST_VALUE(qty) IGNORE NULLS OVER (ORDER BY qty_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        MIN(qty_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)) AS moved_at_month_min_qty_bucket,
        ROUND(PERCENT_RANK() OVER w_all_by_qty_bucket_ties, 3) AS qty_bucket_percent_rank,
        SUM(qty) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)) AS moved_at_month_sum_qty,
        COUNT(*) OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(moved_at, 3) OVER (ORDER BY qty_bucket DESC, id) AS prev3_moved_at,
        SUM(unit_price) OVER w_moved_at_month AS moved_at_month_sum_unit_price,
        LEAD(moved_at) OVER w_warehouse_id_by_qty_bucket AS next_moved_at,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY qty_bucket) AS qty_bucket_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at)) AS rows_in_moved_at_month,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id) AS unit_price_seq,
        LAG(qty, 2) OVER (PARTITION BY warehouse_id ORDER BY unit_price ASC, id) AS prev2_qty
    FROM filtered_moves
    WINDOW w_all_by_qty_bucket_ties AS (ORDER BY qty_bucket DESC), w_moved_at_month AS (PARTITION BY DATE_TRUNC('MONTH', moved_at)), w_warehouse_id_by_qty_bucket AS (PARTITION BY warehouse_id ORDER BY qty_bucket, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY sku_id, warehouse_id) AS rows_in_sku_id_warehouse_id,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id, warehouse_id), 0), 4) AS qty_share_of_sku_id_warehouse_id,
        ROUND(AVG(unit_price_seq) OVER (ORDER BY moved_at_month_sum_qty DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_seq_avg_7,
        MIN(prev2_qty) OVER w_sku_id_warehouse_id AS sku_id_warehouse_id_min_prev2_qty
    FROM ranked
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    unit_price_share_of_moved_at_month,
    moved_at_month_min_qty_bucket,
    qty_bucket_percent_rank,
    running_count,
    prev3_moved_at,
    moved_at_month_sum_unit_price,
    next_moved_at,
    qty_bucket_rank,
    rows_in_sku_id_warehouse_id,
    qty_share_of_sku_id_warehouse_id,
    unit_price_seq_avg_7,
    sku_id_warehouse_id_min_prev2_qty,
    CASE WHEN unit_price_seq >= 8 THEN 'HIGH' WHEN unit_price_seq >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS unit_price_seq_tier
FROM with_trends
WHERE moved_at_month_sum_unit_price < 4500.1
ORDER BY moved_at_month_min_qty_bucket DESC, unit_price_seq_avg_7 DESC

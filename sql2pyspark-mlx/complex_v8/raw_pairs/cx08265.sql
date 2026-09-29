WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE qty <= 387
      AND unit_price < 292.3
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT s.*, sk.unit_cost AS sku_unit_cost
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY sku_unit_cost, id) AS sku_unit_cost_dense_rank,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id)) AS distinct_operator_in_sku_id,
        unit_price_bucket - LAG(unit_price_bucket) OVER (PARTITION BY sku_id ORDER BY sku_unit_cost, id) AS unit_price_bucket_change,
        ROW_NUMBER() OVER w_warehouse_id_operator_by_qty AS qty_seq,
        RANK() OVER w_warehouse_id_operator_by_qty AS qty_rank,
        SUM(qty) OVER (PARTITION BY sku_id) AS sku_id_sum_qty,
        ROUND(CUME_DIST() OVER w_warehouse_id_operator_by_qty_2, 3) AS qty_cume_dist,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY warehouse_id, operator ORDER BY qty NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        ROUND(unit_price_bucket / NULLIF(SUM(unit_price_bucket) OVER (PARTITION BY sku_id), 0), 4) AS unit_price_bucket_share_of_sku_id,
        LAST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty
    FROM joined_sku
    WINDOW w_warehouse_id_operator_by_qty AS (PARTITION BY warehouse_id, operator ORDER BY qty NULLS LAST, id), w_warehouse_id_operator_by_qty_2 AS (PARTITION BY warehouse_id, operator ORDER BY qty NULLS LAST)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE sku_unit_cost_dense_rank <= 2
)
SELECT
    id,
    sku_id,
    qty,
    operator,
    unit_price,
    unit_price_bucket,
    sku_unit_cost,
    sku_unit_cost_dense_rank,
    distinct_operator_in_sku_id,
    unit_price_bucket_change,
    qty_seq,
    qty_rank,
    sku_id_sum_qty,
    unit_price_bucket_share_of_sku_id,
    last_qty,
    CONCAT_WS('-', move_type, operator) AS move_type_operator_label,
    QUARTER(first_known_moved_at) AS first_known_moved_at_quarter
FROM top_rows
ORDER BY first_known_moved_at_quarter, id
LIMIT 20

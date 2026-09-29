WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CASE WHEN qty >= 392 THEN 'A' WHEN qty >= 169 THEN 'B' ELSE 'C' END AS qty_tier,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE warehouse_id <= 4
      AND unit_price < 317.0
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.qty_tier,
        s.operator_len,
        w.opened_on AS warehouse_opened_on
    FROM base_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.opened_on >= DATE '2024-04-13'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY moved_at ASC NULLS LAST, id) AS moved_at_dense_rank,
        SUM(unit_price) OVER w_operator_len_warehouse_id AS operator_len_warehouse_id_sum_unit_price,
        ROUND(AVG(operator_len) OVER (PARTITION BY operator_len, warehouse_id), 2) AS operator_len_warehouse_id_avg_operator_len,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_operator_len_warehouse_id, 0), 4) AS unit_price_share_of_operator_len_warehouse_id,
        MAX(qty) OVER (PARTITION BY operator_len, warehouse_id) AS operator_len_warehouse_id_max_qty,
        LAG(operator_len, 2) OVER (PARTITION BY warehouse_id, move_type ORDER BY operator_len DESC, id) AS prev2_operator_len,
        ROUND(PERCENT_RANK() OVER w_operator_len_qty_tier_by_war, 3) AS warehouse_opened_on_percent_rank,
        ROUND(AVG(unit_price) OVER w_operator_len_warehouse_id, 2) AS operator_len_warehouse_id_avg_unit_price,
        ROUND(operator_len / NULLIF(SUM(operator_len) OVER (PARTITION BY operator_len, warehouse_id), 0), 4) AS operator_len_share_of_operator_len_warehouse_id
    FROM with_details_warehouse
    WINDOW w_operator_len_warehouse_id AS (PARTITION BY operator_len, warehouse_id), w_operator_len_qty_tier_by_war AS (PARTITION BY operator_len, qty_tier ORDER BY warehouse_opened_on DESC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(prev2_operator_len) OVER w_move_type_operator_len, 2) AS move_type_operator_len_avg_prev2_operator_len,
        DENSE_RANK() OVER w_operator_len_move_type_by_wa AS warehouse_opened_on_percent_rank_dense_rank,
        DENSE_RANK() OVER (PARTITION BY qty_tier ORDER BY moved_at) AS moved_at_dense_rank_2,
        NTILE(10) OVER (PARTITION BY operator_len, move_type ORDER BY warehouse_opened_on_percent_rank DESC, id) AS warehouse_opened_on_percent_rank_decile
    FROM ranked
    WINDOW w_move_type_operator_len AS (PARTITION BY move_type, operator_len), w_operator_len_move_type_by_wa AS (PARTITION BY operator_len, move_type ORDER BY warehouse_opened_on_percent_rank DESC)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    qty_tier,
    operator_len,
    warehouse_opened_on,
    moved_at_dense_rank,
    operator_len_warehouse_id_sum_unit_price,
    operator_len_warehouse_id_avg_operator_len,
    unit_price_share_of_operator_len_warehouse_id,
    prev2_operator_len,
    move_type_operator_len_avg_prev2_operator_len,
    warehouse_opened_on_percent_rank_dense_rank,
    moved_at_dense_rank_2,
    warehouse_opened_on_percent_rank_decile,
    CASE WHEN qty >= 348 THEN 'HIGH' WHEN qty >= 123 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier_2,
    moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
FROM metrics
WHERE qty_tier IS NOT NULL
ORDER BY prev2_operator_len, id DESC

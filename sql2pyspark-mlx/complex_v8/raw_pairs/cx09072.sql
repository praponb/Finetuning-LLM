WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE warehouse_id >= 2
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        sku_weight_kg - LAG(sku_weight_kg) OVER (PARTITION BY warehouse_id, YEAR(moved_at) ORDER BY sku_weight_kg, id) AS sku_weight_kg_change,
        DENSE_RANK() OVER w_warehouse_id_by_sku_weight_k AS sku_weight_kg_dense_rank,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY move_type, sku_id), 0), 4) AS qty_share_of_move_type_sku_id,
        NTILE(3) OVER w_warehouse_id_by_sku_weight_k AS sku_weight_kg_tercile,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type, sku_id), 2) AS move_type_sku_id_avg_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY sku_weight_kg DESC, id), 3) AS sku_weight_kg_cume_dist,
        MAX(warehouse_capacity) OVER (PARTITION BY move_type, operator ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS warehouse_capacity_max_2,
        SUM(sku_weight_kg) OVER w_warehouse_id AS warehouse_id_sum_sku_weight_kg,
        LEAD(qty) OVER (PARTITION BY move_type ORDER BY sku_weight_kg DESC, id) AS next_qty
    FROM enriched
    WINDOW w_warehouse_id_by_sku_weight_k AS (PARTITION BY warehouse_id ORDER BY sku_weight_kg DESC, id), w_warehouse_id AS (PARTITION BY warehouse_id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE sku_weight_kg_dense_rank = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        sku_weight_kg,
        sku_code,
        sku_weight_kg_dense_rank,
        qty_share_of_move_type_sku_id,
        sku_weight_kg_tercile,
        sku_weight_kg_cume_dist,
        warehouse_capacity_max_2,
        warehouse_id_sum_sku_weight_kg,
        next_qty,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_weight_kg_tercile ORDER BY unit_price), 3) AS unit_price_cume_dist,
        ROUND(AVG(sku_weight_kg_dense_rank) OVER (PARTITION BY operator ORDER BY sku_weight_kg_cume_dist DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_dense_rank_avg_2_prior,
        COUNT(*) OVER w_warehouse_id_operator AS rows_in_warehouse_id_operator,
        SUM(sku_weight_kg_change) OVER (PARTITION BY sku_weight_kg_tercile ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_weight_kg_change,
        ROUND(qty - AVG(qty) OVER (PARTITION BY warehouse_id, operator), 2) AS qty_vs_warehouse_id_operator_avg
    FROM top_per_group
    WINDOW w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    sku_weight_kg,
    sku_code,
    qty_share_of_move_type_sku_id,
    sku_weight_kg_tercile,
    sku_weight_kg_cume_dist,
    warehouse_capacity_max_2,
    next_qty,
    unit_price_cume_dist,
    rows_in_warehouse_id_operator,
    running_sku_weight_kg_change
FROM sequenced
ORDER BY sku_weight_kg

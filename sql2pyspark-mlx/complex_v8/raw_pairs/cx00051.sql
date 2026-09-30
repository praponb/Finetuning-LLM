WITH cleaned AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 09:00:00'
      AND warehouse_id IS NOT NULL
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.move_type_len,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM cleaned s
    JOIN skus sk ON sk.id = s.sku_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY operator ORDER BY move_type_len ASC) AS move_type_len_dense_rank,
        ROUND(AVG(qty) OVER w_warehouse_id_sku_id, 2) AS warehouse_id_sku_id_avg_qty,
        SUM(qty) OVER (PARTITION BY warehouse_id, sku_id) AS warehouse_id_sku_id_sum_qty,
        LEAD(sku_weight_kg) OVER (PARTITION BY operator ORDER BY move_type_len ASC, id) AS next_sku_weight_kg,
        COUNT(*) OVER w_warehouse_id_sku_id AS rows_in_warehouse_id_sku_id,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY move_type_len DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        RANK() OVER w_sku_id_by_sku_unit_cost_ties AS sku_unit_cost_rank,
        LEAD(sku_unit_cost, 3) OVER (PARTITION BY warehouse_id ORDER BY move_type_len DESC, id) AS next3_sku_unit_cost,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id) AS rn,
        FIRST_VALUE(moved_at) OVER (PARTITION BY warehouse_id ORDER BY move_type_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_moved_at,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY operator ORDER BY move_type_len ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS sku_unit_cost_avg_2,
        LEAD(move_type_len, 2) OVER w_sku_id_by_sku_unit_cost AS next2_move_type_len,
        ROUND(sku_weight_kg - AVG(sku_weight_kg) OVER (PARTITION BY warehouse_id, sku_id), 2) AS sku_weight_kg_vs_warehouse_id_sku_id_avg,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_operator_in_warehouse_id_sku_id,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER w_warehouse_id_sku_id, 2) AS sku_unit_cost_vs_warehouse_id_sku_id_avg
    FROM with_parent_sku
    WINDOW w_warehouse_id_sku_id AS (PARTITION BY warehouse_id, sku_id), w_sku_id_by_sku_unit_cost_ties AS (PARTITION BY sku_id ORDER BY sku_unit_cost DESC NULLS FIRST), w_sku_id_by_sku_unit_cost AS (PARTITION BY sku_id ORDER BY sku_unit_cost DESC NULLS FIRST, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        move_type_len,
        sku_unit_cost,
        sku_weight_kg,
        sku_code,
        move_type_len_dense_rank,
        warehouse_id_sku_id_avg_qty,
        warehouse_id_sku_id_sum_qty,
        next_sku_weight_kg,
        rows_in_warehouse_id_sku_id,
        minutes_since_prev_moved_at,
        sku_unit_cost_rank,
        next3_sku_unit_cost,
        first_moved_at,
        sku_unit_cost_avg_2,
        next2_move_type_len,
        sku_weight_kg_vs_warehouse_id_sku_id_avg,
        distinct_operator_in_warehouse_id_sku_id,
        sku_unit_cost_vs_warehouse_id_sku_id_avg
    FROM ranked
    WHERE rn = 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    move_type_len,
    sku_unit_cost,
    sku_weight_kg,
    move_type_len_dense_rank,
    warehouse_id_sku_id_avg_qty,
    warehouse_id_sku_id_sum_qty,
    next_sku_weight_kg,
    rows_in_warehouse_id_sku_id,
    minutes_since_prev_moved_at,
    sku_unit_cost_rank,
    next3_sku_unit_cost,
    first_moved_at,
    sku_unit_cost_avg_2,
    next2_move_type_len,
    distinct_operator_in_warehouse_id_sku_id
FROM top_rows
WHERE next2_move_type_len <= 13

WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IN ('INBOUND', 'OUTBOUND')
      AND operator = 'op_bee'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM base s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY sku_weight_kg, id) AS sku_weight_kg_rank,
        DENSE_RANK() OVER w_move_type_by_qty_ties AS qty_dense_rank,
        FIRST_VALUE(qty) OVER (ORDER BY sku_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        LAG(qty, 2) OVER (PARTITION BY move_type ORDER BY qty DESC NULLS FIRST, id) AS prev2_qty,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price,
        ROUND(sku_unit_cost - AVG(sku_unit_cost) OVER w_move_type, 2) AS sku_unit_cost_vs_move_type_avg,
        SUM(sku_weight_kg) OVER (PARTITION BY warehouse_id, move_type ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_weight_kg,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type) AS distinct_warehouse_id_in_move_type,
        ROUND(qty - AVG(qty) OVER w_move_type, 2) AS qty_vs_move_type_avg,
        MIN(sku_unit_cost) OVER (PARTITION BY move_type) AS move_type_min_sku_unit_cost,
        LEAD(moved_at) OVER w_move_type_by_qty AS next_moved_at,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY move_type ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        NTILE(3) OVER w_all_by_unit_price AS unit_price_tercile,
        ROUND(AVG(sku_weight_kg) OVER w_move_type, 2) AS move_type_avg_sku_weight_kg
    FROM joined
    WINDOW w_move_type_by_qty_ties AS (PARTITION BY move_type ORDER BY qty DESC NULLS FIRST), w_move_type AS (PARTITION BY move_type), w_move_type_by_qty AS (PARTITION BY move_type ORDER BY qty DESC NULLS FIRST, id), w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        sku_weight_kg,
        qty_dense_rank,
        first_qty,
        running_sku_weight_kg,
        qty_vs_move_type_avg,
        move_type_min_sku_unit_cost,
        next_moved_at,
        second_unit_price,
        unit_price_tercile,
        FIRST_VALUE(move_type_avg_sku_weight_kg) OVER (PARTITION BY sku_weight_kg_rank, YEAR(next_moved_at) ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_type_avg_sku_weight_kg,
        ROUND(AVG(qty_dense_rank) OVER (PARTITION BY sku_id ORDER BY qty_vs_move_type_avg, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_dense_rank_avg_2,
        COUNT(*) OVER w_next_moved_at_month_qty_dens AS rows_in_next_moved_at_month_qty_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty_vs_move_type_avg, id) AS qty_vs_move_type_avg_seq,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_id ORDER BY qty_vs_move_type_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        move_type_min_sku_unit_cost - LAG(move_type_min_sku_unit_cost) OVER (PARTITION BY sku_weight_kg_rank, YEAR(next_moved_at) ORDER BY moved_at, id) AS move_type_min_sku_unit_cost_change,
        SUM(qty_dense_rank) OVER (PARTITION BY sku_weight_kg_rank, YEAR(next_moved_at) ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty_dense_rank
    FROM windowed
    WINDOW w_next_moved_at_month_qty_dens AS (PARTITION BY DATE_TRUNC('MONTH', next_moved_at), qty_dense_rank)
)
SELECT
    id,
    warehouse_id,
    move_type,
    unit_price,
    sku_weight_kg,
    qty_dense_rank,
    running_sku_weight_kg,
    qty_vs_move_type_avg,
    move_type_min_sku_unit_cost,
    second_unit_price,
    unit_price_tercile,
    qty_dense_rank_avg_2,
    rows_in_next_moved_at_month_qty_dense_rank,
    last_moved_at,
    running_qty_dense_rank,
    YEAR(moved_at) AS moved_at_year
FROM windowed_2

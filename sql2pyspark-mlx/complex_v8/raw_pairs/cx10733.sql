WITH recent_moves AS (
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
        QUARTER(moved_at) AS moved_at_quarter,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d
    FROM stock_moves
    WHERE warehouse_id >= 3
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        operator,
        unit_price,
        sku_unit_cost,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost), 3) AS sku_unit_cost_percent_rank,
        SUM(sku_unit_cost) OVER (ORDER BY qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sku_unit_cost_sum_3,
        DENSE_RANK() OVER w_operator_move_type_by_unit_p AS unit_price_dense_rank,
        unit_price - LAG(unit_price) OVER (PARTITION BY warehouse_id, sku_id ORDER BY sku_unit_cost, id) AS unit_price_change,
        COUNT(*) OVER (PARTITION BY sku_id, operator) AS rows_in_sku_id_operator,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER w_warehouse_id, 0), 4) AS sku_unit_cost_share_of_warehouse_id,
        NTILE(5) OVER w_sku_id_operator_by_sku_unit AS sku_unit_cost_quintile,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_operator, 0), 4) AS unit_price_share_of_sku_id_operator,
        LEAD(qty, 1, 0) OVER w_sku_id_operator_by_sku_unit AS next_qty,
        RANK() OVER w_all_by_qty AS qty_rank,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, operator)) AS distinct_warehouse_id_in_sku_id_operator,
        ROUND(CUME_DIST() OVER w_sku_id_operator_by_sku_unit_2, 3) AS sku_unit_cost_cume_dist,
        RANK() OVER (ORDER BY qty ASC) AS qty_rank_2,
        NTILE(4) OVER w_all_by_qty AS qty_quartile
    FROM with_details
    WINDOW w_operator_move_type_by_unit_p AS (PARTITION BY operator, move_type ORDER BY unit_price), w_warehouse_id AS (PARTITION BY warehouse_id), w_sku_id_operator_by_sku_unit AS (PARTITION BY sku_id, operator ORDER BY sku_unit_cost DESC, id), w_sku_id_operator AS (PARTITION BY sku_id, operator), w_all_by_qty AS (ORDER BY qty, id), w_sku_id_operator_by_sku_unit_2 AS (PARTITION BY sku_id, operator ORDER BY sku_unit_cost DESC)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE unit_price_dense_rank = 1
      AND warehouse_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(qty_quartile) OVER (PARTITION BY warehouse_id ORDER BY qty_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_quartile_so_far,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY warehouse_id, qty_rank_2 ORDER BY qty_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(qty_quartile / NULLIF(SUM(qty_quartile) OVER (PARTITION BY unit_price_dense_rank, operator), 0), 4) AS qty_quartile_share_of_unit_price_dense_rank_oper,
        NTILE(5) OVER (PARTITION BY distinct_warehouse_id_in_sku_id_operator, operator ORDER BY next_qty, id) AS next_qty_quintile
    FROM leaders
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    operator,
    unit_price,
    sku_unit_cost,
    sku_unit_cost_percent_rank,
    sku_unit_cost_sum_3,
    unit_price_dense_rank,
    unit_price_change,
    rows_in_sku_id_operator,
    unit_price_share_of_sku_id_operator,
    next_qty,
    distinct_warehouse_id_in_sku_id_operator,
    sku_unit_cost_cume_dist,
    qty_rank_2,
    qty_quartile,
    max_qty_quartile_so_far,
    second_unit_price,
    next_qty_quintile
FROM with_trends
ORDER BY unit_price_share_of_sku_id_operator DESC NULLS FIRST

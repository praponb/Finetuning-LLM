WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix
    FROM stock_moves
    WHERE warehouse_id <= 5
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_prefix,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM prepared s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        move_type,
        qty,
        moved_at,
        operator,
        operator_prefix,
        sku_code,
        sku_unit_cost,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY operator, move_type ORDER BY moved_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS sku_unit_cost_avg_4,
        SUM(qty) OVER (PARTITION BY warehouse_id, TO_DATE(moved_at)) AS warehouse_id_moved_at_to_date_sum_qty,
        COUNT(*) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id) AS unit_price_rank,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator), 2) AS unit_price_vs_operator_avg,
        NTH_VALUE(sku_unit_cost, 2) OVER (PARTITION BY move_type, operator ORDER BY moved_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_unit_cost,
        FIRST_VALUE(qty) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        SUM(unit_price) OVER w_warehouse_id_moved_at_to_dat AS warehouse_id_moved_at_to_date_sum_unit_price,
        ROUND(AVG(qty) OVER w_warehouse_id_moved_at_to_dat, 2) AS warehouse_id_moved_at_to_date_avg_qty
    FROM enriched
    WINDOW w_warehouse_id_moved_at_to_dat AS (PARTITION BY warehouse_id, TO_DATE(moved_at)), w_operator AS (PARTITION BY operator)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(second_sku_unit_cost) OVER (PARTITION BY move_type ORDER BY warehouse_id_moved_at_to_date_avg_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_second_sku_unit_cost_so_far,
        ROW_NUMBER() OVER w_move_type_by_warehouse_id_mo_2 AS warehouse_id_moved_at_to_date_avg_qty_seq,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        NTILE(5) OVER (PARTITION BY move_type ORDER BY warehouse_id_moved_at_to_date_avg_qty DESC, id) AS warehouse_id_moved_at_to_date_avg_qty_quintile,
        FIRST_VALUE(running_count) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY warehouse_id_moved_at_to_date_avg_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count,
        ROUND(unit_price_vs_operator_avg - AVG(unit_price_vs_operator_avg) OVER (PARTITION BY move_type), 2) AS unit_price_vs_operator_avg_vs_move_type_avg
    FROM sequenced
    WINDOW w_move_type_by_warehouse_id_mo_2 AS (PARTITION BY move_type ORDER BY warehouse_id_moved_at_to_date_avg_qty DESC, id)
)
-- Final projection
SELECT
    id,
    qty,
    moved_at,
    operator,
    operator_prefix,
    sku_unit_cost,
    sku_unit_cost_avg_4,
    warehouse_id_moved_at_to_date_sum_qty,
    running_count,
    unit_price_rank,
    unit_price_vs_operator_avg,
    warehouse_id_moved_at_to_date_avg_qty,
    max_second_sku_unit_cost_so_far,
    warehouse_id_moved_at_to_date_avg_qty_seq,
    warehouse_id_moved_at_to_date_avg_qty_quintile,
    first_known_running_count,
    unit_price_vs_operator_avg_vs_move_type_avg
FROM with_trends

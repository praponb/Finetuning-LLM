WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, move_type), 0), 4) AS unit_price_share_of_operator_move_type,
        RANK() OVER w_all_by_unit_price_ties AS unit_price_rank,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id), 0), 4) AS unit_price_share_of_warehouse_id,
        ROUND(AVG(qty) OVER w_warehouse_id, 2) AS warehouse_id_avg_qty,
        NTILE(5) OVER w_warehouse_id_by_qty AS qty_quintile,
        MAX(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_unit_price,
        LAG(unit_price, 3, 0) OVER (ORDER BY unit_price DESC, id) AS prev3_unit_price,
        NTH_VALUE(qty, 2) OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, move_type), 0), 4) AS qty_share_of_operator_move_type,
        SUM(qty) OVER (PARTITION BY sku_id, operator ORDER BY moved_at_day, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior
    FROM cleaned
    WINDOW w_all_by_unit_price_ties AS (ORDER BY unit_price DESC), w_warehouse_id AS (PARTITION BY warehouse_id), w_warehouse_id_by_qty AS (PARTITION BY warehouse_id ORDER BY qty, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        moved_at_day,
        unit_price_share_of_operator_move_type,
        second_unit_price,
        unit_price_share_of_warehouse_id,
        warehouse_id_avg_qty,
        qty_quintile,
        warehouse_id_max_unit_price,
        prev3_unit_price,
        second_qty,
        qty_share_of_operator_move_type,
        qty_sum_2_prior
    FROM sequenced
    WHERE unit_price_rank = 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    moved_at_day,
    unit_price_share_of_operator_move_type,
    second_unit_price,
    unit_price_share_of_warehouse_id,
    warehouse_id_avg_qty,
    qty_quintile,
    warehouse_id_max_unit_price,
    prev3_unit_price,
    second_qty
FROM top_per_group
ORDER BY warehouse_id_avg_qty DESC, second_unit_price

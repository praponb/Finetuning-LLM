WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
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
    WHERE warehouse_id <= 5
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(qty) OVER w_sku_id_by_qty AS prev_qty,
        NTILE(10) OVER (ORDER BY moved_at DESC, id) AS moved_at_decile,
        RANK() OVER (ORDER BY unit_price, id) AS unit_price_rank,
        RANK() OVER (PARTITION BY operator ORDER BY qty) AS qty_rank,
        MIN(unit_price) OVER w_sku_id AS sku_id_min_unit_price,
        NTH_VALUE(unit_price, 2) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY qty), 3) AS qty_percent_rank,
        LAG(qty, 2, 0) OVER (PARTITION BY sku_id ORDER BY qty, id) AS prev2_qty,
        NTH_VALUE(unit_price, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_2,
        ROUND(CUME_DIST() OVER (ORDER BY moved_at DESC, id), 3) AS moved_at_cume_dist,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_id), 0), 4) AS qty_share_of_sku_id,
        NTILE(5) OVER w_sku_id_move_type_by_moved_at_2 AS moved_at_quintile,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id)) AS distinct_operator_in_sku_id,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type, operator), 0), 4) AS unit_price_share_of_move_type_operator
    FROM filtered_moves
    WINDOW w_sku_id_by_qty AS (PARTITION BY sku_id ORDER BY qty, id), w_sku_id AS (PARTITION BY sku_id), w_sku_id_move_type_by_moved_at_2 AS (PARTITION BY sku_id, move_type ORDER BY moved_at, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE qty_rank = 1
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    prev_qty,
    unit_price_rank,
    qty_rank,
    sku_id_min_unit_price,
    second_unit_price,
    qty_percent_rank,
    prev2_qty,
    second_unit_price_2,
    moved_at_cume_dist,
    qty_share_of_sku_id,
    moved_at_quintile,
    distinct_operator_in_sku_id,
    unit_price_share_of_move_type_operator,
    CASE WHEN prev_qty >= 392 THEN 'A' WHEN prev_qty >= 154 THEN 'B' ELSE 'C' END AS prev_qty_tier
FROM first_rows
ORDER BY prev2_qty

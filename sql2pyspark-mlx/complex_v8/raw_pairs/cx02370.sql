WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        ABS(qty - 168) AS qty_distance
    FROM stock_moves
    WHERE operator IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(qty_distance) OVER w_warehouse_id AS warehouse_id_sum_qty_distance,
        RANK() OVER w_sku_id_move_type_by_unit_pri AS unit_price_rank,
        MAX(qty) OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior,
        ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY unit_price DESC, id) AS rn,
        LAG(qty, 2) OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id) AS prev2_qty,
        LEAD(unit_price) OVER (PARTITION BY sku_id, move_type ORDER BY unit_price DESC, id) AS next_unit_price,
        NTILE(5) OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id) AS unit_price_quintile,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id)) AS distinct_move_type_in_warehouse_id,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_id, move_type ORDER BY unit_price DESC), 3) AS unit_price_cume_dist,
        FIRST_VALUE(qty_distance) OVER (PARTITION BY sku_id, operator ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_distance,
        ROW_NUMBER() OVER w_sku_id_operator_by_unit_pric_2 AS seq_no
    FROM filtered
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id), w_sku_id_move_type_by_unit_pri AS (PARTITION BY sku_id, move_type ORDER BY unit_price DESC, id), w_sku_id_operator_by_unit_pric_2 AS (PARTITION BY sku_id, operator ORDER BY unit_price, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    operator,
    unit_price,
    qty_distance,
    warehouse_id_sum_qty_distance,
    unit_price_rank,
    qty_max_2_prior,
    rn,
    prev2_qty,
    next_unit_price,
    first_qty_distance,
    seq_no,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM windowed

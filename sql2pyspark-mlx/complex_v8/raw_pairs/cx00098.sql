WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty BETWEEN 90 AND 349
      AND move_type = 'INBOUND'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(move_type)) AS move_type_clean,
        LEAST(unit_price, qty) AS least_unit_price_qty,
        COALESCE(qty, 0) AS qty_filled
    FROM recent
    WHERE operator IN ('op_chan', 'op_dew', 'op_ann')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        move_type,
        qty,
        operator,
        move_type_clean,
        least_unit_price_qty,
        qty_filled,
        DENSE_RANK() OVER (ORDER BY qty DESC) AS qty_dense_rank,
        least_unit_price_qty - LAG(least_unit_price_qty) OVER w_move_type_operator_by_qty_fi AS least_unit_price_qty_change,
        LAST_VALUE(least_unit_price_qty) IGNORE NULLS OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_unit_price_qty
    FROM scored
    WINDOW w_move_type_operator_by_qty_fi AS (PARTITION BY move_type, operator ORDER BY qty_filled ASC NULLS LAST, id)
),
labeled AS (
    -- Derive helper columns
    SELECT *, ROUND(qty / NULLIF(qty_dense_rank, 0), 3) AS qty_per_qty_dense_rank
    FROM ranked
    WHERE least_unit_price_qty_change >= -350.0
)
-- Final projection
SELECT
    id,
    move_type,
    qty,
    operator,
    move_type_clean,
    qty_filled,
    qty_dense_rank,
    last_known_least_unit_price_qty,
    qty_per_qty_dense_rank,
    CASE WHEN operator IN ('op_dew', 'op_bee') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
    ROUND(qty_per_qty_dense_rank * 0.25, 2) AS qty_per_qty_dense_rank_adj
FROM labeled
ORDER BY qty_dense_rank DESC

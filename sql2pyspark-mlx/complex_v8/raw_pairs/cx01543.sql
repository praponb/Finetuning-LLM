WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        qty + unit_price AS qty_plus_unit_price,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE operator NOT IN ('op_ann', 'op_eve')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY qty, id), 3) AS qty_cume_dist,
        NTH_VALUE(qty_plus_unit_price, 2) OVER (PARTITION BY move_type, operator_or_unknown ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty_plus_unit_price
    FROM cleaned
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator_ref,
        qty_cume_dist,
        second_qty_plus_unit_price,
        MAX(qty) OVER (PARTITION BY move_type) AS move_type_max_qty,
        ROUND(qty_plus_unit_price / NULLIF(SUM(qty_plus_unit_price) OVER (PARTITION BY move_type), 0), 4) AS qty_plus_unit_price_share_of_move_type,
        NTILE(3) OVER w_operator_or_unknown_by_qty_c AS qty_cume_dist_tercile
    FROM sequenced
    WINDOW w_operator_or_unknown_by_qty_c AS (PARTITION BY operator_or_unknown ORDER BY qty_cume_dist ASC, id)
),
annotated AS (
    -- Label and score rows
    SELECT *, FLOOR(qty_cume_dist_tercile / 2) * 2 AS qty_cume_dist_tercile_bucket
    FROM sequenced_2
    WHERE qty_cume_dist < 0.7
)
SELECT
    move_type,
    qty_cume_dist_tercile_bucket,
    GROUPING(move_type) AS is_total_move_type,
    MAX(moved_at) AS last_moved_at,
    PERCENTILE_APPROX(qty_plus_unit_price_share_of_move_type, 0.5) AS median_qty_plus_unit_price_share_of_move_type,
    COUNT(*) AS cnt
FROM annotated
GROUP BY ROLLUP(move_type, qty_cume_dist_tercile_bucket)
ORDER BY move_type NULLS LAST, qty_cume_dist_tercile_bucket NULLS LAST

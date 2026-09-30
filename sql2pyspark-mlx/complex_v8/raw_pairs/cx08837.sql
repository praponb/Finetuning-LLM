WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty >= 99
      AND sku_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator), 2) AS qty_vs_operator_avg,
        ROUND(AVG(qty) OVER (PARTITION BY move_type), 2) AS move_type_avg_qty,
        MAX(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_max_6_prior,
        RANK() OVER (ORDER BY qty DESC, id) AS qty_rank,
        qty - LAG(qty) OVER (ORDER BY unit_price, id) AS qty_change,
        ROUND(CUME_DIST() OVER (ORDER BY qty DESC, id), 3) AS qty_cume_dist,
        LAG(qty, 1, 0) OVER w_all_by_unit_price AS prev_qty,
        RANK() OVER (PARTITION BY operator, sku_id ORDER BY qty DESC) AS qty_rank_2,
        RANK() OVER (ORDER BY qty DESC) AS qty_rank_3,
        ROUND(unit_price - AVG(unit_price) OVER w_operator, 2) AS unit_price_vs_operator_avg,
        FIRST_VALUE(qty) OVER (PARTITION BY operator, sku_id ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        ROUND(AVG(unit_price) OVER w_move_type, 2) AS move_type_avg_unit_price,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY move_type), 0), 4) AS unit_price_share_of_move_type,
        ROUND(AVG(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        COUNT(*) OVER w_move_type AS rows_in_move_type
    FROM prepared
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price, id), w_operator AS (PARTITION BY operator), w_move_type AS (PARTITION BY move_type)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    move_type_avg_qty,
    unit_price_max_6_prior,
    qty_rank,
    qty_change,
    qty_cume_dist,
    qty_rank_3,
    unit_price_vs_operator_avg,
    first_qty,
    qty_avg_1_prior,
    move_type_avg_unit_price,
    unit_price_share_of_move_type,
    qty_avg_3,
    rows_in_move_type,
    ABS(unit_price - 180.0) AS unit_price_distance,
    CONCAT_WS('-', operator, move_type) AS operator_move_type_label
FROM sequenced

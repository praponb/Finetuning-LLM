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
    WHERE (operator IN ('op_dew', 'op_eve') OR sku_id >= 11)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM base
    WHERE unit_price < (SELECT MAX(unit_price) FROM base)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(qty) OVER (PARTITION BY TO_DATE(moved_at), operator ORDER BY unit_price, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_sum_1_prior,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        SUM(qty) OVER (PARTITION BY TO_DATE(moved_at), operator ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_sum_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY qty DESC), 3) AS qty_cume_dist,
        ROUND(PERCENT_RANK() OVER w_moved_at_to_date_operator_by_3, 3) AS unit_price_percent_rank,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(moved_at) ORDER BY qty DESC, id) AS qty_dense_rank
    FROM qualified
    WINDOW w_moved_at_to_date_operator_by_3 AS (PARTITION BY TO_DATE(moved_at), operator ORDER BY unit_price, id)
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.sku_id <= 17
        )
      AND s.qty_cume_dist < 0.7
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    rows_in_move_type,
    qty_sum_2_prior,
    unit_price_percent_rank,
    qty_dense_rank
FROM without_matches
ORDER BY sku_id, moved_at DESC

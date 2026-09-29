WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        TO_DATE(moved_at) AS moved_at_date,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty < 355
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.moved_at_date,
        s.unit_price_adj,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id,
        st.operator AS move_operator
    FROM recent s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(qty) OVER (ORDER BY unit_price_adj DESC, id, move_id) AS next_qty,
        LAST_VALUE(move_unit_price) OVER (PARTITION BY operator, warehouse_id ORDER BY move_unit_price, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_move_unit_price,
        LAG(unit_price_adj) OVER (PARTITION BY operator, warehouse_id ORDER BY move_unit_price, id, move_id) AS prev_unit_price_adj,
        MIN(unit_price_adj) OVER (PARTITION BY operator, warehouse_id ORDER BY move_unit_price, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_adj_so_far
    FROM line_level
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_move_id_by_qty_ties AS qty_rank,
        ROUND(move_unit_price / NULLIF(SUM(move_unit_price) OVER (PARTITION BY move_operator), 0), 4) AS move_unit_price_share_of_move_operator,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_operator)) AS distinct_operator_in_move_operator,
        SUM(move_unit_price) OVER (PARTITION BY move_id ORDER BY qty DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_move_unit_price
    FROM ranked
    WINDOW w_move_id_by_qty_ties AS (PARTITION BY move_id ORDER BY qty DESC)
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND HOUR(st.moved_at) BETWEEN 6 AND 12
              AND st.move_type = 'ADJUST'
        )
      AND s.last_move_unit_price < 396.4
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at_date,
        unit_price_adj,
        move_id,
        move_unit_price,
        move_warehouse_id,
        last_move_unit_price,
        prev_unit_price_adj,
        min_unit_price_adj_so_far,
        distinct_operator_in_move_operator,
        running_move_unit_price,
        SUM(qty_rank) OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_adj DESC, id, move_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_rank_sum_2,
        NTH_VALUE(running_move_unit_price, 2) OVER (ORDER BY moved_at_date, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_move_unit_price,
        SIZE(COLLECT_SET(operator) OVER w_move_id_move_type) AS distinct_operator_in_move_id_move_type,
        ROUND(qty / NULLIF(SUM(qty) OVER w_move_id_move_type, 0), 4) AS qty_share_of_move_id_move_type,
        FIRST_VALUE(running_move_unit_price) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_adj DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_move_unit_price,
        FIRST_VALUE(next_qty) IGNORE NULLS OVER (PARTITION BY move_type, warehouse_id ORDER BY unit_price_adj DESC, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_qty
    FROM without_matches
    WINDOW w_move_id_move_type AS (PARTITION BY move_id, move_type)
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at,
        (SELECT COUNT(*) FILTER (WHERE st.move_type <> 'INBOUND') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count
    FROM ranked_2 s
)
SELECT *
FROM (
    SELECT move_warehouse_id, move_type, min_unit_price_adj_so_far
    FROM with_stats
) src
PIVOT (
    AVG(min_unit_price_adj_so_far) AS avg_v, MAX(min_unit_price_adj_so_far) AS max_v
    FOR move_type IN ('TRANSFER', 'ADJUST')
)
ORDER BY move_warehouse_id

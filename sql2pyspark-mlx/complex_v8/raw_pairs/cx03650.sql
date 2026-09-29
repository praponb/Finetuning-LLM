WITH valid_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        DATE_TRUNC('QUARTER', opened_on) AS opened_on_quarter,
        FLOOR(capacity / 5000) * 5000 AS capacity_bucket
    FROM warehouses
    WHERE YEAR(opened_on) = 2025
      AND wh_code IS NOT NULL
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.capacity,
        s.opened_on,
        s.opened_on_quarter,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at,
        st.move_type
    FROM valid_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id AND st.move_type = 'ADJUST'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM detail_rows_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator <> 'op_eve'
              AND st.move_type IN ('INBOUND', 'TRANSFER')
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_on), TO_DATE(move_moved_at) ORDER BY move_moved_at, id, move_id) AS move_moved_at_seq,
        FIRST_VALUE(opened_on_quarter) IGNORE NULLS OVER (PARTITION BY YEAR(opened_on), TO_DATE(move_moved_at) ORDER BY move_moved_at, id, move_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_on_quarter,
        RANK() OVER (PARTITION BY YEAR(opened_on), TO_DATE(move_moved_at) ORDER BY move_moved_at, id, move_id) AS move_moved_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(opened_on), TO_DATE(move_moved_at) ORDER BY move_moved_at), 3) AS move_moved_at_percent_rank,
        ROUND(AVG(move_unit_price) OVER (PARTITION BY YEAR(opened_on), TO_DATE(move_moved_at) ORDER BY move_moved_at, id, move_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS move_unit_price_avg_6_prior
    FROM filtered
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        opened_on_quarter,
        move_id,
        move_unit_price,
        move_moved_at,
        move_type,
        move_moved_at_seq,
        first_known_opened_on_quarter,
        move_moved_at_percent_rank,
        move_unit_price_avg_6_prior
    FROM windowed
    WHERE move_moved_at_rank = 1
      AND opened_on >= DATE '2024-11-25'
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.opened_on,
        s.opened_on_quarter,
        s.move_id,
        s.move_moved_at,
        s.move_moved_at_seq,
        s.move_moved_at_percent_rank,
        st.id AS move_id_2,
        st.moved_at AS move_moved_at_2,
        st.unit_price AS move_unit_price_2,
        st.sku_id AS move_sku_id,
        st.operator AS move_operator
    FROM top_rows s
    JOIN stock_moves st ON st.warehouse_id = s.id
)
SELECT id, move_id, metric_name, amount
FROM (
    SELECT id, move_id, move_unit_price_2, move_moved_at_percent_rank
    FROM exploded_detail
) src
UNPIVOT (
    amount FOR metric_name IN (move_unit_price_2 AS m_move_unit_price_2, move_moved_at_percent_rank AS m_move_moved_at_percent_rank)
) unp

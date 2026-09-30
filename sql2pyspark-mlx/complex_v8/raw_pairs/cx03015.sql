WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_ann')
),
warehouse_id_stats AS (
    -- Max unit_price per warehouse_id
    SELECT warehouse_id, MAX(unit_price) AS max_unit_price
    FROM recent_moves
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_price,
        ROUND(s.unit_price - g.max_unit_price, 2) AS unit_price_minus_max
    FROM recent_moves s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.unit_price = g.max_unit_price
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        sku_id,
        ROUND(STDDEV(unit_price_minus_max), 2) AS stddev_unit_price_minus_max,
        ROUND(100 * COUNT(CASE WHEN move_type = 'ADJUST' THEN 1 END) / COUNT(*), 1) AS pct_move_type_adjust
    FROM above_average
    WHERE max_unit_price < 325.4
    GROUP BY move_type, sku_id
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 387.1
        )
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.move_type,
        s.sku_id,
        s.stddev_unit_price_minus_max,
        s.pct_move_type_adjust,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.moved_at AS move_moved_at
    FROM screened s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id AND st.warehouse_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(pct_move_type_adjust) OVER (PARTITION BY TO_DATE(move_moved_at), sku_id ORDER BY move_moved_at DESC NULLS FIRST, move_type, sku_id, move_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pct_move_type_adjust_max_2_prior,
        ROUND((UNIX_TIMESTAMP(move_moved_at) - UNIX_TIMESTAMP(LAG(move_moved_at) OVER w_move_moved_at_to_date_sku_id_2)) / 60, 1) AS minutes_since_prev_move_moved_at,
        DENSE_RANK() OVER w_move_moved_at_month_by_stdde AS stddev_unit_price_minus_max_dense_rank
    FROM detail_rows_moves
    WINDOW w_move_moved_at_to_date_sku_id_2 AS (PARTITION BY TO_DATE(move_moved_at), sku_id ORDER BY move_moved_at DESC NULLS FIRST, move_type, sku_id, move_id), w_move_moved_at_month_by_stdde AS (PARTITION BY MONTH(move_moved_at) ORDER BY stddev_unit_price_minus_max NULLS LAST)
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.move_warehouse_id
              AND w.capacity > 4912
        )
)
SELECT move_type, sku_id, measure, value
FROM (
    SELECT move_type, sku_id, pct_move_type_adjust_max_2_prior, minutes_since_prev_move_moved_at
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (pct_move_type_adjust_max_2_prior AS m_pct_move_type_adjust_max_2_prior, minutes_since_prev_move_moved_at AS minutes_since_prev_move_moved_at_v)
) unp

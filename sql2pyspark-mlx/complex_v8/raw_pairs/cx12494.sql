WITH filtered AS (
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
        qty + unit_price AS qty_plus_unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        ABS(qty - 191) AS qty_distance
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(qty) OVER (PARTITION BY move_type_len ORDER BY move_type_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_type_len ORDER BY move_type_len DESC, id), 3) AS move_type_len_percent_rank,
        move_type_len - LAG(move_type_len) OVER (PARTITION BY move_type_len ORDER BY move_type_len DESC, id) AS move_type_len_change,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY move_type_len ORDER BY move_type_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        MAX(unit_price) OVER (PARTITION BY move_type_len ORDER BY move_type_len DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_max_7
    FROM filtered
),
move_type_stats AS (
    -- Avg qty_distance per move_type
    SELECT move_type, AVG(qty_distance) AS avg_qty_distance
    FROM sequenced
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_qty_distance,
        ROUND(s.qty_distance - g.avg_qty_distance, 2) AS qty_distance_minus_avg
    FROM sequenced s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty_distance > g.avg_qty_distance
),
filtered_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
      AND s.sku_id >= 11
)
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    qty_plus_unit_price,
    move_type_len,
    qty_distance,
    last_qty,
    move_type_len_change,
    second_moved_at,
    unit_price_max_7,
    avg_qty_distance,
    qty_distance_minus_avg,
    SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
FROM filtered_2
ORDER BY avg_qty_distance DESC, move_type DESC NULLS FIRST, id
LIMIT 10

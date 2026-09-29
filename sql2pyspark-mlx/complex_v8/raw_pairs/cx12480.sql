WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE unit_price BETWEEN 58.6 AND 337.6
      AND warehouse_id >= 3
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM cleaned s
),
warehouse_id_stats AS (
    -- Max move_count per warehouse_id
    SELECT warehouse_id, MAX(move_count) AS max_move_count
    FROM with_counts
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_move_count,
        ROUND(s.move_count - g.max_move_count, 2) AS move_count_minus_max
    FROM with_counts s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.move_count = g.max_move_count
),
joined AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.least_qty_unit_price,
        s.moved_at_hour,
        s.move_count,
        s.move_count_minus_max,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM above_average s
    JOIN warehouses w ON w.id = s.warehouse_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.warehouse_wh_code,
        s.warehouse_capacity,
        s.warehouse_opened_on,
        st.id AS move_id,
        st.qty AS move_qty,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at
    FROM joined s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    warehouse_wh_code,
    warehouse_capacity,
    warehouse_opened_on,
    move_id,
    move_qty,
    move_sku_id,
    move_moved_at,
    CASE WHEN move_unit_price >= 363.6 THEN 'HIGH' WHEN move_unit_price >= 124.1 THEN 'MEDIUM' ELSE 'LOW' END AS move_unit_price_tier,
    moved_at + INTERVAL 2 HOURS AS moved_at_plus_2h
FROM line_level
ORDER BY moved_at, id, move_id
LIMIT 5

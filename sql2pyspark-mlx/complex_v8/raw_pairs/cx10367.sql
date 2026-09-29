WITH base AS (
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
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-06 08:00:00'
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.operator_ref,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(w.wh_code)) LIKE 'B%'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        operator,
        unit_price,
        operator_ref,
        warehouse_opened_on,
        warehouse_wh_code,
        LENGTH(TRIM(move_type)) AS move_type_len,
        TO_DATE(moved_at) AS moved_at_date,
        TO_DATE(moved_at) AS moved_at_date_2
    FROM with_parent
    WHERE operator_ref IS NOT NULL
),
warehouse_id_stats AS (
    -- Avg move_type_len per warehouse_id
    SELECT warehouse_id, AVG(move_type_len) AS avg_move_type_len
    FROM derived
    GROUP BY warehouse_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_type_len,
        ROUND(s.move_type_len - g.avg_move_type_len, 2) AS move_type_len_minus_avg
    FROM derived s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.move_type_len > g.avg_move_type_len
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, MAX(unit_price) AS max_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.max_move_unit_price
    FROM above_average s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
warehouse_id_stats_2 AS (
    -- Median max_move_unit_price per warehouse_id
    SELECT warehouse_id, PERCENTILE_APPROX(max_move_unit_price, 0.5) AS median_max_move_unit_price
    FROM profiled
    GROUP BY warehouse_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_move_unit_price,
        ROUND(s.max_move_unit_price - g.median_max_move_unit_price, 2) AS max_move_unit_price_minus_median
    FROM profiled s
    JOIN warehouse_id_stats_2 g ON g.warehouse_id = s.warehouse_id
    WHERE s.max_move_unit_price >= g.median_max_move_unit_price
)
-- Final projection
SELECT
    id,
    warehouse_id,
    unit_price,
    operator_ref,
    warehouse_wh_code,
    move_type_len,
    avg_move_type_len,
    move_type_len_minus_avg,
    max_move_unit_price,
    median_max_move_unit_price,
    max_move_unit_price_minus_median,
    ABS(move_type_len_minus_avg - -1.4) AS move_type_len_minus_avg_distance,
    COALESCE(move_type_len, 0) AS move_type_len_filled
FROM outliers

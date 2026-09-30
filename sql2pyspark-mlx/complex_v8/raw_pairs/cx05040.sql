WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price < 273.5
),
move_type_stats AS (
    -- Median unit_price per move_type
    SELECT move_type, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM prepared_moves
    GROUP BY move_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM prepared_moves s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price >= g.median_unit_price
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(median_unit_price, 0.0) AS median_unit_price_filled
    FROM above_average
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY operator), 2) AS unit_price_vs_operator_avg,
        COUNT(*) OVER (PARTITION BY sku_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM labeled
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.median_unit_price,
        s.unit_price_vs_operator_avg,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.qty AS move_qty,
        st.operator AS move_operator
    FROM metrics s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.qty >= 124
),
with_children_2 AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.median_unit_price,
        s.move_id,
        st.id AS move_id_2,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price
    FROM with_children s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    median_unit_price,
    move_id,
    move_id_2,
    move_moved_at,
    move_unit_price
FROM with_children_2
ORDER BY move_type, id, move_id, move_id_2
LIMIT 10

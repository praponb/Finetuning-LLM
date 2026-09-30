WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(unit_price - 293.2) AS unit_price_distance,
        COALESCE(qty, 0) AS qty_filled,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE operator = 'op_bee'
),
latest_per_key AS (
    -- Keep the latest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_distance,
        qty_filled,
        days_since_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY qty DESC, id DESC) AS recency_rank
        FROM prepared_moves
    ) ranked
    WHERE recency_rank = 1
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.moved_at AS move_moved_at
    FROM latest_per_key s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
sku_id_stats AS (
    -- Avg move_unit_price per sku_id
    SELECT sku_id, AVG(move_unit_price) AS avg_move_unit_price
    FROM line_level_moves
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_move_unit_price,
        ROUND(s.move_unit_price - g.avg_move_unit_price, 2) AS move_unit_price_minus_avg
    FROM line_level_moves s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.move_unit_price > g.avg_move_unit_price
)
-- Subtotals with ROLLUP
SELECT
    move_id,
    COALESCE(move_type, 'ALL') AS move_type,
    warehouse_id,
    ROUND(SUM(move_unit_price_minus_avg), 2) AS total_move_unit_price_minus_avg,
    ROUND(STDDEV(move_unit_price), 2) AS stddev_move_unit_price,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
FROM vs_group
GROUP BY ROLLUP(move_id, move_type, warehouse_id)
ORDER BY move_id NULLS LAST, move_type NULLS LAST, warehouse_id NULLS LAST

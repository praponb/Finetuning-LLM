WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE city = 'Bangkok'
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned_warehouses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type NOT IN ('ADJUST', 'TRANSFER')
        )
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count_2
    FROM without_matches s
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.capacity_filled,
        s.move_count,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty
    FROM with_counts s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
deduped AS (
    -- Keep the latest row per move_operator
    SELECT
        id,
        wh_code,
        city,
        capacity_filled,
        move_count,
        move_id,
        move_unit_price,
        move_operator,
        move_sku_id,
        move_qty
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_operator ORDER BY move_count DESC, id DESC, move_id DESC) AS recency_rank
        FROM line_level_moves
    ) ranked
    WHERE recency_rank = 1
),
move_operator_stats AS (
    -- Max move_unit_price per move_operator
    SELECT move_operator, MAX(move_unit_price) AS max_move_unit_price
    FROM deduped
    GROUP BY move_operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_move_unit_price,
        ROUND(s.move_unit_price - g.max_move_unit_price, 2) AS move_unit_price_minus_max
    FROM deduped s
    JOIN move_operator_stats g ON g.move_operator = s.move_operator
    WHERE s.move_unit_price = g.max_move_unit_price
)
SELECT
    id,
    wh_code,
    city,
    capacity_filled,
    move_count,
    move_unit_price,
    move_operator,
    move_sku_id,
    move_qty,
    max_move_unit_price,
    move_unit_price_minus_max,
    LOWER(TRIM(wh_code)) AS wh_code_clean,
    COALESCE(move_qty, 0) AS move_qty_filled
FROM outliers
ORDER BY id DESC

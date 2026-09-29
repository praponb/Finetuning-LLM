WITH base AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city NOT IN ('Kuala Lumpur', 'Singapore')
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        st.id AS move_id,
        st.qty AS move_qty,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.move_type
    FROM base s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        s.move_id,
        s.move_qty,
        s.move_operator,
        s.move_unit_price,
        s.move_type,
        st.id AS move_id_2,
        st.qty AS move_qty_2,
        st.operator AS move_operator_2,
        st.move_type AS move_move_type
    FROM exploded_detail_moves s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
grouped AS (
    -- Summarise per group
    SELECT
        move_operator_2,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_ym,
        SUM(CASE WHEN move_type IS NOT NULL THEN 1 ELSE 0 END) AS n_move_type_is_not_null,
        ROUND(AVG(capacity), 2) AS avg_capacity,
        ROUND(SUM(move_qty) / NULLIF(SUM(move_qty_2), 0), 3) AS move_qty_to_move_qty_2_ratio,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE move_move_type IS NOT NULL) AS n_move_move_type_is_not_null
    FROM exploded_detail
    GROUP BY 1, 2
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(move_operator_2, 'UNKNOWN') AS move_operator_2_or_unknown,
        UPPER(TRIM(move_operator_2)) AS move_operator_2_clean
    FROM grouped
)
-- Final projection
SELECT
    move_operator_2,
    opened_on_ym,
    n_move_type_is_not_null,
    avg_capacity,
    move_qty_to_move_qty_2_ratio,
    row_count,
    n_move_move_type_is_not_null,
    move_operator_2_or_unknown,
    move_operator_2_clean
FROM annotated
ORDER BY n_move_move_type_is_not_null, move_operator_2, opened_on_ym
LIMIT 20

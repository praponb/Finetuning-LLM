WITH scoped AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT_WS(' / ', city, wh_code) AS city_wh_code_label,
        LOWER(REGEXP_REPLACE(TRIM(wh_code), ' ', '_')) AS wh_code_slug,
        DATE_TRUNC('MONTH', opened_on) AS opened_on_month
    FROM warehouses
    WHERE city <> 'Kuala Lumpur'
      AND capacity > 4912
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type = 'TRANSFER'
        )
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on_month,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.move_type
    FROM filtered s
    JOIN stock_moves st ON st.warehouse_id = s.id AND st.unit_price >= 150.7
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.operator NOT IN ('op_dew', 'op_eve')
        )
)
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    move_id,
    GROUPING(move_type) AS is_total_move_type,
    ROUND(STDDEV(move_unit_price), 2) AS stddev_move_unit_price,
    MAX(capacity) AS max_capacity
FROM with_activity
GROUP BY GROUPING SETS ((move_type, move_id), (move_type), (move_id), ())
ORDER BY move_type NULLS LAST, move_id NULLS LAST

WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        CONCAT(city, '#', CAST(capacity AS STRING)) AS city_ref,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE city IN ('Singapore', 'Kuala Lumpur')
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_warehouses s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.move_type = 'TRANSFER'
        )
      AND s.capacity >= 2614
),
scored AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(wh_code)) AS wh_code_len
    FROM filtered
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.capacity,
        s.opened_on,
        s.city_ref,
        s.wh_code_len,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.move_type,
        st.unit_price AS move_unit_price
    FROM scored s
    JOIN stock_moves st ON st.warehouse_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT opened_on, move_id, move_type, capacity
    FROM with_children_moves
) src
PIVOT (
    AVG(capacity) AS avg_v, COUNT(capacity) AS n
    FOR move_type IN ('ADJUST', 'OUTBOUND', 'TRANSFER')
)
ORDER BY opened_on, move_id

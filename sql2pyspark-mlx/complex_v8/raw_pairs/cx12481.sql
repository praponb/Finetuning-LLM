WITH valid AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN capacity >= 11506 THEN 'HIGH' WHEN capacity >= 11005 THEN 'MEDIUM' ELSE 'LOW' END AS capacity_tier
    FROM valid
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.unit_price <= 313.8
        )
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.wh_code,
        s.city,
        s.opened_on,
        s.capacity_tier,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM qualified s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.id AND HOUR(st.moved_at) BETWEEN 7 AND 13
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT capacity_tier, city, move_qty
    FROM line_level
) src
PIVOT (
    MAX(move_qty)
    FOR city IN ('Singapore', 'Bangkok')
)
ORDER BY capacity_tier

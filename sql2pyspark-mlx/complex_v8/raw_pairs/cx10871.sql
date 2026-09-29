WITH valid AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
      AND LENGTH(TRIM(wh_code)) >= 7
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.qty < 275
        )
)
SELECT
    id,
    wh_code,
    city,
    capacity,
    INITCAP(TRIM(wh_code)) AS wh_code_clean
FROM without_matches
ORDER BY wh_code_clean DESC, capacity DESC

WITH cleaned_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        DATE_FORMAT(opened_on, 'yyyy-MM-dd') AS opened_on_label,
        DATE_FORMAT(opened_on, 'yyyy-MM-dd') AS opened_on_label_2
    FROM warehouses
    WHERE city <> 'Bangkok'
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned_warehouses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND MONTH(st.moved_at) IN (1, 2)
              AND st.move_type IN ('TRANSFER', 'INBOUND', 'OUTBOUND')
        )
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
              AND st.sku_id IS NOT NULL
        )
)
SELECT
    id,
    wh_code,
    opened_on_label,
    LENGTH(TRIM(opened_on_label_2)) AS opened_on_label_2_len
FROM filtered
ORDER BY wh_code DESC, opened_on_label_2_len NULLS LAST, id
LIMIT 10

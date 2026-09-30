WITH cleaned AS (
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
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 7 AND 10
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty < 348
              AND st.unit_price < 290.4
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        UPPER(TRIM(move_type)) AS move_type_clean,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM screened
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    operator,
    unit_price,
    moved_at_year,
    move_type_or_unknown,
    move_type_clean,
    qty_bucket,
    DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
FROM tagged
ORDER BY unit_price NULLS LAST, warehouse_id, id
LIMIT 20

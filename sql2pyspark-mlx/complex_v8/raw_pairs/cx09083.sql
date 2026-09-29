WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LENGTH(TRIM(operator)) AS operator_len,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        CASE WHEN qty >= 288 THEN 'A' WHEN qty >= 124 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE operator <> 'op_eve'
      AND sku_id IS NOT NULL
),
filtered_2 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND TO_DATE(st.moved_at) < DATE '2025-02-24'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    COALESCE(operator, 'ALL') AS operator,
    warehouse_id,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(operator_len), 2) AS stddev_operator_len
FROM filtered_2
GROUP BY GROUPING SETS ((move_type, operator, warehouse_id), (move_type), ())
ORDER BY move_type NULLS LAST, operator NULLS LAST, warehouse_id NULLS LAST

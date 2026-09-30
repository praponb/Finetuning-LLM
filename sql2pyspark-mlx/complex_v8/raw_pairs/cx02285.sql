WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND qty BETWEEN 124 AND 463
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_bee', 'op_ann')
        )
      AND s.operator_move_type_label IS NOT NULL
)
SELECT
    operator,
    move_type,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM filtered
GROUP BY GROUPING SETS ((operator, move_type), (operator), ())
ORDER BY operator NULLS LAST, move_type NULLS LAST

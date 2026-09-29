WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        CASE WHEN operator IN ('op_dew', 'op_bee') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE warehouse_id >= 3
      AND operator = 'op_bee'
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE qty > (SELECT MIN(qty) FROM filtered)
),
filtered_3 AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 323.9
              AND st.move_type <> 'OUTBOUND'
        )
)
SELECT
    operator_group,
    warehouse_id,
    sku_id,
    PERCENTILE_APPROX(qty, 0.5) AS median_qty,
    MAX(moved_at) AS last_moved_at,
    COUNT(*) AS cnt
FROM filtered_3
GROUP BY GROUPING SETS ((operator_group, warehouse_id, sku_id), (operator_group), (warehouse_id), ())
ORDER BY operator_group NULLS LAST, warehouse_id NULLS LAST, sku_id NULLS LAST

WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
),
labeled AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM scoped
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE qty > (SELECT AVG(qty) FROM labeled)
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    operator,
    GROUPING(sku_id) AS is_total_sku_id,
    ROUND(STDDEV(qty), 2) AS stddev_qty
FROM qualified
GROUP BY GROUPING SETS ((sku_id, operator), (sku_id))
ORDER BY sku_id NULLS LAST, operator NULLS LAST

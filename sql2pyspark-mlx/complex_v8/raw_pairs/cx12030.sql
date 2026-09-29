WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        unit_cost,
        labels,
        COALESCE(unit_cost, 0.0) AS unit_cost_filled,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM skus
    WHERE category = 'Toys'
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.operator IN ('op_ann', 'op_bee', 'op_eve')
              AND st.unit_price BETWEEN 152.7 AND 293.2
        )
      AND s.category_or_unknown = 'Toys'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT category_or_unknown, category, unit_cost_filled
    FROM screened
) src
PIVOT (
    SUM(unit_cost_filled)
    FOR category IN ('Toys', 'Food', 'Electronics')
)
ORDER BY category_or_unknown

WITH scoped AS (
    -- Filter skus
    SELECT *
    FROM skus
    WHERE category = 'Apparel'
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.qty > 138
        )
)
-- Subtotals with GROUPING SETS
SELECT
    is_active,
    category,
    SUM(weight_kg) AS total_weight_kg,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg,
    MIN(weight_kg) AS min_weight_kg
FROM with_activity
GROUP BY GROUPING SETS ((is_active, category), (is_active), (category))
ORDER BY is_active NULLS LAST, category NULLS LAST

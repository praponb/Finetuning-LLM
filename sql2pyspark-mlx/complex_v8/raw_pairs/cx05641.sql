WITH base_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown_2
    FROM skus
    WHERE unit_cost >= 134.3
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_skus s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.move_type IN ('OUTBOUND', 'TRANSFER', 'INBOUND')
        )
      AND s.weight_kg >= 8.5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        category,
        is_active,
        category_or_unknown,
        ROUND(AVG(weight_kg) OVER (PARTITION BY category_or_unknown_2), 2) AS category_or_unknown_2_avg_weight_kg,
        ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY weight_kg, id) AS seq_no,
        ROUND(weight_kg - AVG(weight_kg) OVER w_category_or_unknown_2, 2) AS weight_kg_vs_category_or_unknown_2_avg
    FROM without_matches
    WINDOW w_category_or_unknown_2 AS (PARTITION BY category_or_unknown_2)
),
tagged AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(category_or_unknown)) AS category_or_unknown_clean
    FROM sequenced
)
-- Final projection
SELECT
    id,
    sku_code,
    category_or_unknown,
    category_or_unknown_2_avg_weight_kg,
    weight_kg_vs_category_or_unknown_2_avg,
    category_or_unknown_clean
FROM tagged
WHERE LOWER(category_or_unknown_clean) LIKE '%le%'
ORDER BY category_or_unknown_2_avg_weight_kg DESC, sku_code DESC

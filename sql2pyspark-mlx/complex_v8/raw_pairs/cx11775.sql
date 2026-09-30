WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        CASE WHEN weight_kg >= 18.5 THEN 'A' WHEN weight_kg >= 12.4 THEN 'B' ELSE 'C' END AS weight_kg_tier,
        CONCAT_WS(': ', sku_code, category) AS sku_code_category_label
    FROM skus
    WHERE SIZE(labels) >= 2
),
category_stats AS (
    -- Median weight_kg per category
    SELECT category, PERCENTILE_APPROX(weight_kg, 0.5) AS median_weight_kg
    FROM cleaned_skus
    GROUP BY category
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_weight_kg,
        ROUND(s.weight_kg - g.median_weight_kg, 2) AS weight_kg_minus_median
    FROM cleaned_skus s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg >= g.median_weight_kg
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.qty > 91
              AND st.unit_price < 330.0
        )
      AND s.weight_kg >= 6.4
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(weight_kg_minus_median) OVER (PARTITION BY category, weight_kg_tier) AS category_weight_kg_tier_min_weight_kg_minus_medi,
        ROUND(weight_kg - AVG(weight_kg) OVER (PARTITION BY category, weight_kg_tier), 2) AS weight_kg_vs_category_weight_kg_tier_avg,
        ROUND(weight_kg_minus_median - AVG(weight_kg_minus_median) OVER (PARTITION BY category, weight_kg_tier), 2) AS weight_kg_minus_median_vs_category_weight_kg_tie
    FROM without_matches
)
SELECT *
FROM (
    SELECT category, weight_kg_tier, weight_kg_vs_category_weight_kg_tier_avg
    FROM windowed
) src
PIVOT (
    MIN(weight_kg_vs_category_weight_kg_tier_avg)
    FOR weight_kg_tier IN ('B', 'A', 'C')
)

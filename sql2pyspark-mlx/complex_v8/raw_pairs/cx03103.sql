WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE is_active = TRUE
      AND unit_cost <= 320.4
),
category_stats AS (
    -- Avg weight_kg per category
    SELECT category, AVG(weight_kg) AS avg_weight_kg
    FROM base
    GROUP BY category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_weight_kg,
        ROUND(s.weight_kg - g.avg_weight_kg, 2) AS weight_kg_minus_avg
    FROM base s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg > g.avg_weight_kg
),
category_stats_2 AS (
    -- Avg weight_kg per category
    SELECT category, AVG(weight_kg) AS avg_weight_kg_2
    FROM vs_group
    GROUP BY category
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_weight_kg_2,
        ROUND(s.weight_kg - g.avg_weight_kg_2, 2) AS weight_kg_minus_avg_2
    FROM vs_group s
    JOIN category_stats_2 g ON g.category = s.category
    WHERE s.weight_kg > g.avg_weight_kg_2
)
-- Subtotals with GROUPING SETS
SELECT
    category,
    is_active,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
    ROUND(SUM(weight_kg_minus_avg_2) / NULLIF(SUM(unit_cost), 0), 3) AS weight_kg_minus_avg_2_to_unit_cost_ratio
FROM outliers
GROUP BY GROUPING SETS ((category, is_active), (category), (is_active))
ORDER BY category NULLS LAST, is_active NULLS LAST

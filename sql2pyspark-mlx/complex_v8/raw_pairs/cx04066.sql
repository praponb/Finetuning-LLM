WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        ROUND(weight_kg * 1.5, 2) AS weight_kg_adj,
        CASE WHEN unit_cost >= 302.5 THEN 'large' WHEN unit_cost >= 134.3 THEN 'medium' ELSE 'small' END AS unit_cost_tier
    FROM skus
    WHERE category IN ('Beauty', 'Electronics')
      AND unit_cost > 68.0
),
deduped AS (
    -- Keep the latest row per unit_cost_tier
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        weight_kg_adj,
        unit_cost_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY unit_cost_tier ORDER BY unit_cost DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
category_stats AS (
    -- Avg weight_kg per category
    SELECT category, AVG(weight_kg) AS avg_weight_kg
    FROM deduped
    GROUP BY category
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_weight_kg,
        ROUND(s.weight_kg - g.avg_weight_kg, 2) AS weight_kg_minus_avg
    FROM deduped s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg > g.avg_weight_kg
),
latest AS (
    -- Keep the latest row per category, unit_cost_tier
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        weight_kg_adj,
        unit_cost_tier,
        avg_weight_kg,
        weight_kg_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category, unit_cost_tier ORDER BY avg_weight_kg DESC, id DESC) AS row_num
        FROM outliers
    ) ranked
    WHERE row_num = 1
)
SELECT
    is_active,
    COALESCE(category, 'ALL') AS category,
    COALESCE(unit_cost_tier, 'ALL') AS unit_cost_tier,
    COUNT(*) FILTER (WHERE is_active = TRUE) AS n_is_active_true
FROM latest
GROUP BY CUBE(is_active, category, unit_cost_tier)

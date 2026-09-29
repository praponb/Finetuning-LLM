WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN weight_kg >= 20.8 THEN 'A' WHEN weight_kg >= 6.5 THEN 'B' ELSE 'C' END AS weight_kg_tier,
        SUBSTRING(TRIM(category), 1, 2) AS category_prefix,
        SUBSTRING(TRIM(category), 1, 2) AS category_prefix_2
    FROM skus
    WHERE is_active = TRUE
      AND NOT ARRAY_CONTAINS(labels, 'fragile')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(unit_cost) OVER (PARTITION BY category) AS category_max_unit_cost,
        weight_kg - LAG(weight_kg) OVER (ORDER BY unit_cost DESC, id) AS weight_kg_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_cost), 3) AS unit_cost_percent_rank,
        ROUND(unit_cost - AVG(unit_cost) OVER (PARTITION BY category), 2) AS unit_cost_vs_category_avg,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY category), 0), 4) AS weight_kg_share_of_category,
        SUM(unit_cost) OVER (ORDER BY unit_cost DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_cost_sum_6_prior
    FROM base
),
category_stats AS (
    -- Max weight_kg_share_of_category per category
    SELECT category, MAX(weight_kg_share_of_category) AS max_weight_kg_share_of_category
    FROM windowed
    GROUP BY category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_weight_kg_share_of_category,
        ROUND(s.weight_kg_share_of_category - g.max_weight_kg_share_of_category, 2) AS weight_kg_share_of_category_minus_max
    FROM windowed s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg_share_of_category = g.max_weight_kg_share_of_category
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM vs_group
    WHERE unit_cost_vs_category_avg > (SELECT MIN(unit_cost_vs_category_avg) FROM vs_group)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE unit_cost > (SELECT AVG(unit_cost) FROM qualified)
)
-- Final projection
SELECT
    id,
    category,
    unit_cost,
    weight_kg,
    labels,
    weight_kg_tier,
    category_prefix,
    category_prefix_2,
    weight_kg_change,
    unit_cost_percent_rank,
    unit_cost_vs_category_avg,
    weight_kg_share_of_category,
    unit_cost_sum_6_prior,
    weight_kg_share_of_category_minus_max,
    CONCAT(weight_kg_tier, '#', CAST(id AS STRING)) AS weight_kg_tier_ref,
    ABS(unit_cost_percent_rank - 0.7) AS unit_cost_percent_rank_distance
FROM filtered
ORDER BY weight_kg_change, id
LIMIT 10

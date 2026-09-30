WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ARRAY_CONTAINS(labels, 'bulky') AS has_bulky,
        weight_kg + unit_cost AS weight_kg_plus_unit_cost,
        CASE WHEN weight_kg >= 15.2 THEN 'A' WHEN weight_kg >= 12.4 THEN 'B' ELSE 'C' END AS weight_kg_tier
    FROM skus
    WHERE LOWER(sku_code) LIKE '%ty%'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(weight_kg_plus_unit_cost) OVER (PARTITION BY category) AS category_sum_weight_kg_plus_unit_cost,
        FIRST_VALUE(unit_cost) OVER (PARTITION BY is_active, category ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_cost,
        ROUND(AVG(weight_kg) OVER (PARTITION BY category), 2) AS category_avg_weight_kg,
        COUNT(*) OVER w_category AS rows_in_category
    FROM scoped
    WINDOW w_category AS (PARTITION BY category)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        has_bulky,
        weight_kg_plus_unit_cost,
        weight_kg_tier,
        category_sum_weight_kg_plus_unit_cost,
        first_unit_cost,
        rows_in_category,
        COUNT(*) OVER (PARTITION BY is_active, weight_kg_tier) AS rows_in_is_active_weight_kg_tier,
        SIZE(COLLECT_SET(category) OVER (PARTITION BY is_active, weight_kg_tier)) AS distinct_category_in_is_active_weight_kg_tier,
        LAST_VALUE(category_avg_weight_kg) IGNORE NULLS OVER (PARTITION BY weight_kg_tier, category ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_category_avg_weight_kg
    FROM sequenced
),
summary AS (
    -- Group-level totals
    SELECT
        is_active,
        SUM(CASE WHEN category IN ('Beauty', 'Apparel') THEN 1 ELSE 0 END) AS n_category_in_beauty_apparel,
        COUNT(*) AS row_count,
        ROUND(SUM(first_unit_cost) / NULLIF(SUM(category_sum_weight_kg_plus_unit_cost), 0), 3) AS first_unit_cost_to_category_sum_weight_kg_plus_u
    FROM with_windows
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE n_category_in_beauty_apparel > (SELECT AVG(n_category_in_beauty_apparel) FROM summary)
),
scored AS (
    -- Derive helper columns
    SELECT
        is_active,
        row_count,
        first_unit_cost_to_category_sum_weight_kg_plus_u,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ABS(row_count - 8) AS row_count_distance,
        FLOOR(row_count / 2) * 2 AS row_count_bucket
    FROM filtered
    WHERE n_category_in_beauty_apparel >= 4
)
SELECT *
FROM (
    SELECT row_count_distance, row_count, is_active_label, row_count_bucket
    FROM scored
) src
PIVOT (
    MAX(row_count_bucket)
    FOR is_active_label IN ('yes', 'no')
)
ORDER BY row_count_distance, row_count

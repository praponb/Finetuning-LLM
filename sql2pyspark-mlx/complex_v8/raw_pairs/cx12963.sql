WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM skus
    WHERE LOWER(sku_code) LIKE '%y-%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_code,
        category,
        labels,
        is_active,
        MAX(unit_cost) OVER (PARTITION BY category, is_active) AS category_is_active_max_unit_cost,
        COUNT(*) OVER (PARTITION BY category, is_active) AS rows_in_category_is_active,
        ROUND(AVG(unit_cost) OVER (PARTITION BY is_active ORDER BY weight_kg, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_cost_avg_3,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
        LAG(unit_cost, 3, 0) OVER (ORDER BY unit_cost DESC, id) AS prev3_unit_cost,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER w_is_active_category, 0), 4) AS weight_kg_share_of_is_active_category,
        FIRST_VALUE(unit_cost) OVER (PARTITION BY category ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_cost,
        RANK() OVER (PARTITION BY category ORDER BY unit_cost DESC NULLS FIRST) AS unit_cost_rank,
        RANK() OVER w_is_active_category_by_weight AS weight_kg_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_active ORDER BY unit_cost), 3) AS unit_cost_percent_rank,
        SUM(unit_cost) OVER (PARTITION BY is_active, category) AS is_active_category_sum_unit_cost
    FROM base
    WINDOW w_is_active_category AS (PARTITION BY is_active, category), w_is_active_category_by_weight AS (PARTITION BY is_active, category ORDER BY weight_kg)
)
-- Final projection
SELECT
    id,
    sku_code,
    category,
    labels,
    weight_kg_share_of_is_active_category,
    first_unit_cost,
    unit_cost_rank,
    weight_kg_rank,
    unit_cost_percent_rank,
    is_active_category_sum_unit_cost,
    UPPER(TRIM(sku_code)) AS sku_code_clean
FROM ranked

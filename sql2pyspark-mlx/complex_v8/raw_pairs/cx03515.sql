WITH recent_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        CASE WHEN unit_cost >= 251.2 THEN 'A' WHEN unit_cost >= 68.0 THEN 'B' ELSE 'C' END AS unit_cost_tier,
        LEAST(weight_kg, unit_cost) AS least_weight_kg_unit_cost
    FROM skus
    WHERE is_active = TRUE
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        unit_cost_tier,
        least_weight_kg_unit_cost,
        DENSE_RANK() OVER w_unit_cost_tier_category_by_l AS least_weight_kg_unit_cost_dense_rank,
        MAX(unit_cost) OVER w_category AS category_max_unit_cost,
        SUM(unit_cost) OVER (PARTITION BY unit_cost_tier, category) AS unit_cost_tier_category_sum_unit_cost,
        MAX(least_weight_kg_unit_cost) OVER (ORDER BY least_weight_kg_unit_cost DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS least_weight_kg_unit_cost_max_3,
        COUNT(*) OVER w_unit_cost_tier AS rows_in_unit_cost_tier,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category, unit_cost_tier ORDER BY unit_cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_6_prior,
        MIN(least_weight_kg_unit_cost) OVER w_unit_cost_tier AS unit_cost_tier_min_least_weight_kg_unit_cost,
        LAG(least_weight_kg_unit_cost, 3) OVER w_unit_cost_tier_category_by_l AS prev3_least_weight_kg_unit_cost,
        RANK() OVER w_unit_cost_tier_category_by_l_2 AS least_weight_kg_unit_cost_rank
    FROM recent_skus
    WINDOW w_unit_cost_tier_category_by_l AS (PARTITION BY unit_cost_tier, category ORDER BY least_weight_kg_unit_cost, id), w_category AS (PARTITION BY category), w_unit_cost_tier AS (PARTITION BY unit_cost_tier), w_unit_cost_tier_category_by_l_2 AS (PARTITION BY unit_cost_tier, category ORDER BY least_weight_kg_unit_cost DESC, id)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(least_weight_kg_unit_cost) OVER (PARTITION BY least_weight_kg_unit_cost_rank, least_weight_kg_unit_cost_dense_rank ORDER BY least_weight_kg_unit_cost_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_weight_kg_unit_cost,
        ROUND(AVG(unit_cost) OVER (PARTITION BY least_weight_kg_unit_cost_rank, least_weight_kg_unit_cost_dense_rank ORDER BY least_weight_kg_unit_cost_dense_rank DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_3_prior,
        ROUND(category_max_unit_cost / NULLIF(SUM(category_max_unit_cost) OVER (PARTITION BY least_weight_kg_unit_cost_dense_rank), 0), 4) AS category_max_unit_cost_share_of_least_weight_kg_,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER w_least_weight_kg_unit_cost_de, 0), 4) AS unit_cost_share_of_least_weight_kg_unit_cost_den,
        NTILE(10) OVER w_least_weight_kg_unit_cost_de_2 AS rows_in_unit_cost_tier_decile
    FROM sequenced
    WINDOW w_least_weight_kg_unit_cost_de AS (PARTITION BY least_weight_kg_unit_cost_dense_rank), w_least_weight_kg_unit_cost_de_2 AS (PARTITION BY least_weight_kg_unit_cost_dense_rank, least_weight_kg_unit_cost_rank ORDER BY rows_in_unit_cost_tier DESC, id)
)
-- Final projection
SELECT
    id,
    sku_code,
    unit_cost,
    least_weight_kg_unit_cost,
    least_weight_kg_unit_cost_dense_rank,
    unit_cost_tier_category_sum_unit_cost,
    unit_cost_avg_6_prior,
    unit_cost_tier_min_least_weight_kg_unit_cost,
    prev3_least_weight_kg_unit_cost,
    least_weight_kg_unit_cost_rank,
    first_least_weight_kg_unit_cost,
    unit_cost_share_of_least_weight_kg_unit_cost_den
FROM sequenced_2

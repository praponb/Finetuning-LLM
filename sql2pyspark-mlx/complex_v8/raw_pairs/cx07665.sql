WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN weight_kg >= 20.8 THEN 'large' WHEN weight_kg >= 12.4 THEN 'medium' ELSE 'small' END AS weight_kg_tier,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'hazmat')
      AND is_active = FALSE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_cost,
        weight_kg,
        labels,
        weight_kg_tier,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER (PARTITION BY is_active), 0), 4) AS weight_kg_share_of_is_active,
        DENSE_RANK() OVER w_is_active_by_unit_cost_per_w AS unit_cost_per_weight_kg_dense_rank,
        ROUND(AVG(unit_cost_per_weight_kg) OVER (ORDER BY unit_cost_per_weight_kg NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_cost_per_weight_kg_avg_4,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (ORDER BY unit_cost_per_weight_kg NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
        LEAD(unit_cost, 1, 0) OVER (ORDER BY unit_cost_per_weight_kg, id) AS next_unit_cost,
        COUNT(*) OVER w_is_active AS rows_in_is_active,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_cost_per_weight_kg), 3) AS unit_cost_per_weight_kg_percent_rank,
        unit_cost_per_weight_kg - LAG(unit_cost_per_weight_kg) OVER (ORDER BY unit_cost_per_weight_kg NULLS LAST, id) AS unit_cost_per_weight_kg_change,
        LAST_VALUE(unit_cost) OVER (ORDER BY unit_cost_per_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        NTILE(10) OVER (PARTITION BY is_active ORDER BY unit_cost_per_weight_kg ASC NULLS LAST, id) AS unit_cost_per_weight_kg_decile,
        FIRST_VALUE(weight_kg) OVER (ORDER BY unit_cost_per_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg
    FROM cleaned_skus
    WINDOW w_is_active_by_unit_cost_per_w AS (PARTITION BY is_active ORDER BY unit_cost_per_weight_kg ASC NULLS LAST), w_is_active AS (PARTITION BY is_active)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE unit_cost_per_weight_kg_dense_rank <= 3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        unit_cost,
        labels,
        weight_kg_tier,
        weight_kg_share_of_is_active,
        last_known_weight_kg,
        next_unit_cost,
        rows_in_is_active,
        unit_cost_per_weight_kg_percent_rank,
        unit_cost_per_weight_kg_change,
        LAG(unit_cost, 3) OVER (PARTITION BY unit_cost_per_weight_kg_dense_rank, unit_cost_per_weight_kg_decile ORDER BY unit_cost_per_weight_kg_change DESC NULLS FIRST, id) AS prev3_unit_cost,
        ROUND(AVG(unit_cost) OVER (PARTITION BY unit_cost_per_weight_kg_dense_rank, unit_cost_per_weight_kg_decile ORDER BY unit_cost_per_weight_kg_change DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_2_prior,
        MAX(last_unit_cost) OVER (PARTITION BY unit_cost_per_weight_kg_dense_rank ORDER BY weight_kg DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last_unit_cost_max_2_prior
    FROM top_rows
)
-- Final projection
SELECT
    id,
    unit_cost,
    labels,
    weight_kg_share_of_is_active,
    last_known_weight_kg,
    next_unit_cost,
    rows_in_is_active,
    unit_cost_per_weight_kg_percent_rank,
    unit_cost_per_weight_kg_change,
    unit_cost_avg_2_prior,
    GREATEST(last_known_weight_kg, rows_in_is_active) AS greatest_last_known_weight_kg_rows_in_is_active,
    LENGTH(TRIM(weight_kg_tier)) AS weight_kg_tier_len
FROM with_trends
WHERE weight_kg_tier = 'large'
ORDER BY unit_cost_per_weight_kg_percent_rank DESC, unit_cost_per_weight_kg_change DESC, id
LIMIT 20

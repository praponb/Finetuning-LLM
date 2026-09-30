WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        unit_cost,
        labels,
        CASE WHEN weight_kg >= 14.6 THEN 'A' WHEN weight_kg >= 12.4 THEN 'B' ELSE 'C' END AS weight_kg_tier
    FROM skus
    WHERE unit_cost > 38.3
      AND ARRAY_CONTAINS(labels, 'hazmat')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_cost_ties, 3) AS unit_cost_percent_rank,
        LAG(unit_cost) OVER (ORDER BY unit_cost DESC, id) AS prev_unit_cost,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY weight_kg_tier), 0), 4) AS unit_cost_share_of_weight_kg_tier,
        COUNT(*) OVER (PARTITION BY weight_kg_tier) AS rows_in_weight_kg_tier,
        unit_cost - LAG(unit_cost) OVER w_all_by_unit_cost AS unit_cost_change,
        COUNT(*) OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM cleaned
    WINDOW w_all_by_unit_cost_ties AS (ORDER BY unit_cost DESC), w_all_by_unit_cost AS (ORDER BY unit_cost DESC, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prev_unit_cost, 3) OVER w_weight_kg_tier_by_unit_cost AS next3_prev_unit_cost,
        COUNT(*) OVER (PARTITION BY weight_kg_tier) AS rows_in_weight_kg_tier_2,
        SUM(prev_unit_cost) OVER (PARTITION BY weight_kg_tier) AS weight_kg_tier_sum_prev_unit_cost
    FROM with_trends
    WINDOW w_weight_kg_tier_by_unit_cost AS (PARTITION BY weight_kg_tier ORDER BY unit_cost_change ASC, id)
)
-- Final projection
SELECT
    id,
    unit_cost,
    labels,
    weight_kg_tier,
    unit_cost_percent_rank,
    prev_unit_cost,
    rows_in_weight_kg_tier,
    running_count,
    next3_prev_unit_cost,
    rows_in_weight_kg_tier_2,
    weight_kg_tier_sum_prev_unit_cost
FROM with_windows
ORDER BY id DESC, weight_kg_tier DESC

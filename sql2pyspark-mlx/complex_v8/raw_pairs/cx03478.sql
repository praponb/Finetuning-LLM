WITH filtered AS (
    -- Filter skus
    SELECT
        id,
        category,
        unit_cost,
        labels,
        is_active,
        ROUND(weight_kg * 0.25, 2) AS weight_kg_adj
    FROM skus
    WHERE unit_cost > 38.3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        category,
        unit_cost,
        weight_kg_adj,
        NTILE(3) OVER (PARTITION BY is_active ORDER BY unit_cost, id) AS unit_cost_tercile,
        ROUND(PERCENT_RANK() OVER w_is_active_by_unit_cost, 3) AS unit_cost_percent_rank,
        NTH_VALUE(weight_kg_adj, 2) OVER (PARTITION BY category ORDER BY weight_kg_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg_adj,
        unit_cost - LAG(unit_cost) OVER (ORDER BY weight_kg_adj DESC NULLS FIRST, id) AS unit_cost_change,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category,
        MAX(weight_kg_adj) OVER (PARTITION BY category) AS category_max_weight_kg_adj,
        ROUND(unit_cost - AVG(unit_cost) OVER w_category, 2) AS unit_cost_vs_category_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_active ORDER BY unit_cost), 3) AS unit_cost_percent_rank_2
    FROM filtered
    WINDOW w_is_active_by_unit_cost AS (PARTITION BY is_active ORDER BY unit_cost, id), w_category AS (PARTITION BY category)
)
SELECT
    id,
    category,
    weight_kg_adj,
    unit_cost_tercile,
    unit_cost_percent_rank,
    second_weight_kg_adj,
    unit_cost_change,
    rows_in_category,
    category_max_weight_kg_adj,
    unit_cost_vs_category_avg,
    unit_cost_percent_rank_2,
    ROUND(rows_in_category / NULLIF(unit_cost_percent_rank_2, 0), 3) AS rows_in_category_per_unit_cost_percent_rank_2,
    SUBSTRING(TRIM(category), 1, 4) AS category_prefix
FROM ranked
WHERE unit_cost_percent_rank > 0.2
ORDER BY unit_cost_percent_rank_2 DESC NULLS FIRST, category NULLS LAST

WITH base AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        SUBSTRING(TRIM(category), 1, 2) AS category_prefix
    FROM skus
    WHERE weight_kg >= 6.4
      AND is_active = TRUE
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        unit_cost,
        is_active,
        category_prefix,
        ROUND(AVG(unit_cost) OVER (ORDER BY unit_cost NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_3_prior,
        ROUND(AVG(weight_kg) OVER (PARTITION BY is_active), 2) AS is_active_avg_weight_kg,
        DENSE_RANK() OVER (ORDER BY unit_cost NULLS LAST) AS unit_cost_dense_rank,
        ROUND(weight_kg - AVG(weight_kg) OVER w_is_active, 2) AS weight_kg_vs_is_active_avg,
        LAG(unit_cost, 1, 0) OVER (ORDER BY unit_cost NULLS LAST, id) AS prev_unit_cost,
        DENSE_RANK() OVER (PARTITION BY is_active ORDER BY unit_cost, id) AS unit_cost_dense_rank_2,
        unit_cost - LAG(unit_cost) OVER (ORDER BY unit_cost NULLS LAST, id) AS unit_cost_change,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER w_is_active, 0), 4) AS unit_cost_share_of_is_active,
        MAX(unit_cost) OVER (PARTITION BY is_active) AS is_active_max_unit_cost,
        FIRST_VALUE(weight_kg) OVER (PARTITION BY is_active ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg,
        LEAD(weight_kg, 1, 0) OVER (ORDER BY unit_cost NULLS LAST, id) AS next_weight_kg
    FROM base
    WINDOW w_is_active AS (PARTITION BY is_active)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        is_active,
        category_prefix,
        is_active_avg_weight_kg,
        unit_cost_dense_rank,
        unit_cost_dense_rank_2,
        unit_cost_change,
        unit_cost_share_of_is_active,
        is_active_max_unit_cost,
        first_weight_kg,
        next_weight_kg,
        ROUND(PERCENT_RANK() OVER w_unit_cost_dense_rank_unit_co, 3) AS is_active_max_unit_cost_percent_rank,
        ROUND(prev_unit_cost - AVG(prev_unit_cost) OVER w_unit_cost_dense_rank, 2) AS prev_unit_cost_vs_unit_cost_dense_rank_avg,
        ROUND(unit_cost_dense_rank_2 - AVG(unit_cost_dense_rank_2) OVER (PARTITION BY unit_cost_dense_rank), 2) AS unit_cost_dense_rank_2_vs_unit_cost_dense_rank_a,
        ROUND(is_active_max_unit_cost / NULLIF(SUM(is_active_max_unit_cost) OVER (PARTITION BY unit_cost_dense_rank), 0), 4) AS is_active_max_unit_cost_share_of_unit_cost_dense
    FROM ranked
    WINDOW w_unit_cost_dense_rank_unit_co AS (PARTITION BY unit_cost_dense_rank, unit_cost_dense_rank_2 ORDER BY is_active_max_unit_cost DESC), w_unit_cost_dense_rank AS (PARTITION BY unit_cost_dense_rank)
)
SELECT
    id,
    sku_code,
    is_active,
    category_prefix,
    is_active_avg_weight_kg,
    unit_cost_dense_rank_2,
    unit_cost_share_of_is_active,
    is_active_max_unit_cost,
    first_weight_kg,
    next_weight_kg,
    is_active_max_unit_cost_percent_rank,
    unit_cost_dense_rank_2_vs_unit_cost_dense_rank_a
FROM windowed
ORDER BY next_weight_kg

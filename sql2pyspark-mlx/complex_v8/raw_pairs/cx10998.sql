WITH valid_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active
    FROM skus
    WHERE category IN ('Food', 'Electronics')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        LAST_VALUE(weight_kg) OVER (ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_weight_kg,
        FIRST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY category ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg,
        DENSE_RANK() OVER w_is_active_by_weight_kg_ties AS weight_kg_dense_rank,
        LAG(unit_cost) OVER (PARTITION BY is_active, category ORDER BY unit_cost, id) AS prev_unit_cost,
        COUNT(*) OVER (PARTITION BY is_active) AS rows_in_is_active,
        RANK() OVER w_all_by_unit_cost AS unit_cost_rank,
        ROUND(weight_kg - AVG(weight_kg) OVER (PARTITION BY is_active), 2) AS weight_kg_vs_is_active_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY category ORDER BY weight_kg, id), 3) AS weight_kg_cume_dist,
        FIRST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY is_active, category ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg_2,
        MIN(unit_cost) OVER (ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_cost_so_far,
        MAX(unit_cost) OVER (PARTITION BY is_active) AS is_active_max_unit_cost,
        LEAD(unit_cost, 3) OVER (PARTITION BY is_active, category ORDER BY unit_cost, id) AS next3_unit_cost,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER w_is_active, 0), 4) AS unit_cost_share_of_is_active,
        ROUND(CUME_DIST() OVER (ORDER BY unit_cost), 3) AS unit_cost_cume_dist
    FROM valid_skus
    WINDOW w_is_active_by_weight_kg_ties AS (PARTITION BY is_active ORDER BY weight_kg DESC), w_all_by_unit_cost AS (ORDER BY unit_cost, id), w_is_active AS (PARTITION BY is_active)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        is_active,
        last_weight_kg,
        first_known_weight_kg,
        prev_unit_cost,
        rows_in_is_active,
        unit_cost_rank,
        weight_kg_vs_is_active_avg,
        weight_kg_cume_dist,
        first_known_weight_kg_2,
        min_unit_cost_so_far,
        is_active_max_unit_cost,
        next3_unit_cost,
        unit_cost_share_of_is_active,
        unit_cost_cume_dist
    FROM sequenced
    WHERE weight_kg_dense_rank <= 5
)
SELECT
    id,
    unit_cost,
    weight_kg,
    is_active,
    last_weight_kg,
    rows_in_is_active,
    weight_kg_vs_is_active_avg,
    weight_kg_cume_dist,
    first_known_weight_kg_2,
    min_unit_cost_so_far,
    is_active_max_unit_cost,
    next3_unit_cost,
    unit_cost_share_of_is_active,
    unit_cost_cume_dist
FROM top_per_group
ORDER BY weight_kg_cume_dist, rows_in_is_active NULLS LAST, id
LIMIT 10

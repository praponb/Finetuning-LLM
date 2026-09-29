WITH valid_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        unit_cost,
        labels,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
    FROM skus
    WHERE SIZE(labels) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        category,
        unit_cost,
        labels,
        MIN(unit_cost) OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_cost_so_far,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (ORDER BY is_active_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        SUM(is_active_flag) OVER (ORDER BY is_active_flag DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS is_active_flag_sum_1_prior,
        ROW_NUMBER() OVER (PARTITION BY category, is_active_flag ORDER BY unit_cost ASC, id) AS unit_cost_seq,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        SUM(unit_cost) OVER (PARTITION BY category, is_active_flag) AS category_is_active_flag_sum_unit_cost,
        ROUND(AVG(is_active_flag) OVER (PARTITION BY category ORDER BY unit_cost, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS is_active_flag_avg_1_prior,
        LAG(unit_cost, 2) OVER (ORDER BY is_active_flag DESC, id) AS prev2_unit_cost,
        MIN(unit_cost) OVER (PARTITION BY category, is_active_flag) AS category_is_active_flag_min_unit_cost,
        MAX(unit_cost) OVER (PARTITION BY is_active_flag, category ORDER BY unit_cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_cost_max_2_prior,
        DENSE_RANK() OVER (ORDER BY unit_cost ASC NULLS LAST) AS unit_cost_dense_rank,
        LEAD(unit_cost, 2) OVER (PARTITION BY category ORDER BY unit_cost, id) AS next2_unit_cost
    FROM valid_skus
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN category IN ('Food', 'Toys') THEN 'group_a' WHEN category = 'Beauty' THEN 'group_b' ELSE 'other' END AS category_group
    FROM ranked
)
SELECT
    id,
    category,
    unit_cost,
    labels,
    min_unit_cost_so_far,
    first_known_unit_cost,
    is_active_flag_sum_1_prior,
    unit_cost_seq,
    last_known_unit_cost,
    category_is_active_flag_sum_unit_cost,
    is_active_flag_avg_1_prior,
    category_is_active_flag_min_unit_cost,
    unit_cost_max_2_prior,
    unit_cost_dense_rank,
    next2_unit_cost,
    GREATEST(unit_cost_seq, min_unit_cost_so_far) AS greatest_unit_cost_seq_min_unit_cost_so_far,
    CASE WHEN last_known_unit_cost >= 277.0 THEN 'HIGH' WHEN last_known_unit_cost >= 158.7 THEN 'MEDIUM' ELSE 'LOW' END AS last_known_unit_cost_tier
FROM tagged

WITH base AS (
    -- Filter skus
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE category = 'Beauty'
      AND unit_cost >= 68.0
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_cost,
        is_active,
        MAX(weight_kg) OVER (PARTITION BY is_active ORDER BY unit_cost DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_weight_kg_so_far,
        LEAD(weight_kg) OVER (ORDER BY unit_cost DESC, id) AS next_weight_kg,
        unit_cost - LAG(unit_cost) OVER w_all_by_unit_cost AS unit_cost_change,
        ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY unit_cost DESC NULLS FIRST, id) AS unit_cost_seq,
        ROUND(PERCENT_RANK() OVER w_all_by_unit_cost_ties, 3) AS unit_cost_percent_rank,
        MAX(unit_cost) OVER (ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_cost_so_far,
        LEAD(weight_kg, 2) OVER (PARTITION BY is_active ORDER BY unit_cost DESC NULLS FIRST, id) AS next2_weight_kg,
        DENSE_RANK() OVER (ORDER BY unit_cost DESC, id) AS unit_cost_dense_rank,
        MIN(weight_kg) OVER (PARTITION BY category, is_active) AS category_is_active_min_weight_kg
    FROM base
    WINDOW w_all_by_unit_cost AS (ORDER BY unit_cost DESC, id), w_all_by_unit_cost_ties AS (ORDER BY unit_cost DESC)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        unit_cost_change,
        unit_cost_seq,
        unit_cost_percent_rank,
        max_unit_cost_so_far,
        next2_weight_kg,
        category_is_active_min_weight_kg,
        ROUND(unit_cost_seq / NULLIF(max_weight_kg_so_far, 0), 3) AS unit_cost_seq_per_max_weight_kg_so_far,
        GREATEST(unit_cost_dense_rank, next2_weight_kg) AS greatest_unit_cost_dense_rank_next2_weight_kg
    FROM metrics
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(max_unit_cost_so_far, 2) OVER (PARTITION BY unit_cost_seq ORDER BY category_is_active_min_weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_unit_cost_so_far,
        ROUND(next2_weight_kg / NULLIF(SUM(next2_weight_kg) OVER (PARTITION BY unit_cost_seq), 0), 4) AS next2_weight_kg_share_of_unit_cost_seq,
        ROUND(CUME_DIST() OVER w_unit_cost_seq_by_category_is_2, 3) AS category_is_active_min_weight_kg_cume_dist,
        ROUND(max_unit_cost_so_far / NULLIF(SUM(max_unit_cost_so_far) OVER (PARTITION BY unit_cost_seq), 0), 4) AS max_unit_cost_so_far_share_of_unit_cost_seq,
        ROUND(AVG(unit_cost_seq) OVER (PARTITION BY unit_cost_seq), 2) AS unit_cost_seq_avg_unit_cost_seq
    FROM derived
    WINDOW w_unit_cost_seq_by_category_is_2 AS (PARTITION BY unit_cost_seq ORDER BY category_is_active_min_weight_kg DESC, id)
)
-- Final projection
SELECT
    id,
    unit_cost_change,
    unit_cost_seq,
    unit_cost_percent_rank,
    max_unit_cost_so_far,
    next2_weight_kg,
    category_is_active_min_weight_kg,
    greatest_unit_cost_dense_rank_next2_weight_kg,
    second_max_unit_cost_so_far,
    next2_weight_kg_share_of_unit_cost_seq,
    category_is_active_min_weight_kg_cume_dist,
    max_unit_cost_so_far_share_of_unit_cost_seq,
    GREATEST(unit_cost_seq, unit_cost_percent_rank) AS greatest_unit_cost_seq_unit_cost_percent_rank
FROM sequenced
ORDER BY unit_cost_seq

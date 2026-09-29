WITH cleaned_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        FLOOR(weight_kg / 5) * 5 AS weight_kg_bucket,
        LOWER(TRIM(category)) AS category_clean
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'A%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category ORDER BY weight_kg_bucket DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS unit_cost_avg_3_prior,
        weight_kg_bucket - LAG(weight_kg_bucket) OVER (PARTITION BY category ORDER BY unit_cost ASC, id) AS weight_kg_bucket_change,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_cost DESC, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category,
        unit_cost - LAG(unit_cost) OVER (PARTITION BY category ORDER BY unit_cost DESC, id) AS unit_cost_change,
        SUM(unit_cost) OVER (PARTITION BY category ORDER BY weight_kg_bucket DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_cost_sum_3_prior,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        RANK() OVER w_category_by_weight_kg_bucket_2 AS weight_kg_bucket_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY category ORDER BY weight_kg_bucket DESC NULLS FIRST, id), 3) AS weight_kg_bucket_cume_dist,
        FIRST_VALUE(weight_kg_bucket) IGNORE NULLS OVER (PARTITION BY category ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg_bucket,
        LAST_VALUE(weight_kg_bucket) OVER (PARTITION BY category ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_weight_kg_bucket
    FROM cleaned_skus
    WINDOW w_category_by_weight_kg_bucket_2 AS (PARTITION BY category ORDER BY weight_kg_bucket DESC NULLS FIRST, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        category,
        unit_cost,
        labels,
        weight_kg_bucket,
        category_clean,
        weight_kg_bucket_change,
        seq_no,
        unit_cost_change,
        last_known_unit_cost,
        weight_kg_bucket_rank,
        weight_kg_bucket_cume_dist,
        first_known_weight_kg_bucket,
        ROUND(unit_cost_sum_3_prior - AVG(unit_cost_sum_3_prior) OVER (PARTITION BY category, seq_no), 2) AS unit_cost_sum_3_prior_vs_category_seq_no_avg,
        FIRST_VALUE(weight_kg_bucket) OVER (PARTITION BY weight_kg_bucket_rank, category ORDER BY weight_kg_bucket_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_weight_kg_bucket,
        COUNT(*) OVER w_category_seq_no AS rows_in_category_seq_no,
        ROUND(first_known_weight_kg_bucket - AVG(first_known_weight_kg_bucket) OVER (PARTITION BY category, seq_no), 2) AS first_known_weight_kg_bucket_vs_category_seq_no_,
        NTH_VALUE(unit_cost_sum_3_prior, 2) OVER (PARTITION BY category, weight_kg_bucket_rank ORDER BY last_weight_kg_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost_sum_3_prior,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY category, seq_no), 2) AS seq_no_vs_category_seq_no_avg
    FROM with_windows
    WINDOW w_category_seq_no AS (PARTITION BY category, seq_no)
)
-- Final projection
SELECT
    id,
    unit_cost,
    labels,
    category_clean,
    weight_kg_bucket_change,
    unit_cost_change,
    last_known_unit_cost,
    weight_kg_bucket_rank,
    weight_kg_bucket_cume_dist,
    first_known_weight_kg_bucket,
    unit_cost_sum_3_prior_vs_category_seq_no_avg,
    first_weight_kg_bucket,
    first_known_weight_kg_bucket_vs_category_seq_no_,
    second_unit_cost_sum_3_prior,
    LENGTH(TRIM(category)) AS category_len
FROM ranked
ORDER BY first_weight_kg_bucket NULLS LAST, id
LIMIT 20

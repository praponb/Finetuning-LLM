WITH base_skus AS (
    -- Filter skus
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown,
        LENGTH(TRIM(category)) AS category_len
    FROM skus
    WHERE LENGTH(TRIM(sku_code)) >= 5
      AND NOT ARRAY_CONTAINS(labels, 'express')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_cost,
        category_len,
        ROUND(CUME_DIST() OVER (PARTITION BY category_len ORDER BY category_len), 3) AS category_len_cume_dist,
        DENSE_RANK() OVER w_category_by_weight_kg_ties AS weight_kg_dense_rank,
        ROUND(PERCENT_RANK() OVER w_category_or_unknown_by_categ, 3) AS category_len_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY category_len ORDER BY category_len, id), 3) AS category_len_cume_dist_2,
        SUM(weight_kg) OVER (PARTITION BY category, category_or_unknown) AS category_category_or_unknown_sum_weight_kg,
        category_len - LAG(category_len) OVER w_category_len_by_category_len_2 AS category_len_change,
        ROUND(category_len - AVG(category_len) OVER w_category_or_unknown, 2) AS category_len_vs_category_or_unknown_avg,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category, category_or_unknown), 2) AS category_category_or_unknown_avg_unit_cost,
        NTH_VALUE(category_len, 2) OVER (PARTITION BY category_or_unknown ORDER BY category_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_category_len,
        LEAD(unit_cost) OVER w_category_or_unknown_by_categ AS next_unit_cost,
        ROUND(AVG(weight_kg) OVER (PARTITION BY category, category_or_unknown), 2) AS category_category_or_unknown_avg_weight_kg,
        ROUND(CUME_DIST() OVER (PARTITION BY category_or_unknown ORDER BY category_len DESC), 3) AS category_len_cume_dist_3,
        LAG(unit_cost) OVER w_category_or_unknown_by_categ_4 AS prev_unit_cost
    FROM base_skus
    WINDOW w_category_by_weight_kg_ties AS (PARTITION BY category ORDER BY weight_kg), w_category_or_unknown_by_categ AS (PARTITION BY category_or_unknown ORDER BY category_len DESC, id), w_category_len_by_category_len_2 AS (PARTITION BY category_len ORDER BY category_len, id), w_category_or_unknown AS (PARTITION BY category_or_unknown), w_category_or_unknown_by_categ_4 AS (PARTITION BY category_or_unknown ORDER BY category_len, id)
)
-- Final projection
SELECT
    id,
    unit_cost,
    category_len_cume_dist,
    weight_kg_dense_rank,
    category_len_percent_rank,
    category_len_cume_dist_2,
    category_category_or_unknown_sum_weight_kg,
    category_len_change,
    category_len_vs_category_or_unknown_avg,
    second_category_len,
    category_len_cume_dist_3,
    prev_unit_cost,
    category_len_cume_dist_3 + category_len_cume_dist AS category_len_cume_dist_3_plus_category_len_cume_,
    category_len_cume_dist_3 - unit_cost AS category_len_cume_dist_3_minus_unit_cost
FROM metrics
ORDER BY weight_kg_dense_rank DESC NULLS FIRST, category_len_cume_dist_3_plus_category_len_cume_

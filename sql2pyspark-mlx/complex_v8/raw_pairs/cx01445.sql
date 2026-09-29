WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        SUBSTRING(TRIM(sku_code), 1, 2) AS sku_code_prefix
    FROM skus
    WHERE category = 'Apparel'
      AND UPPER(TRIM(sku_code)) LIKE 'A%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        sku_code_prefix,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER w_category, 0), 4) AS unit_cost_share_of_category,
        COUNT(*) OVER w_category AS rows_in_category,
        ROUND(AVG(weight_kg) OVER (PARTITION BY category ORDER BY weight_kg, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS weight_kg_avg_3_prior,
        LAST_VALUE(unit_cost) OVER (PARTITION BY category ORDER BY weight_kg ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        LAST_VALUE(unit_cost) OVER (ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost_2,
        SUM(unit_cost) OVER (PARTITION BY category ORDER BY weight_kg ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_cost_sum_2_prior,
        MAX(unit_cost) OVER (PARTITION BY category) AS category_max_unit_cost,
        LAST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY category ORDER BY weight_kg ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg
    FROM scoped
    WINDOW w_category AS (PARTITION BY category)
),
derived AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(sku_code_prefix)) AS sku_code_prefix_clean
    FROM with_windows
    WHERE category_max_unit_cost < 2800.3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        unit_cost,
        sku_code_prefix,
        unit_cost_share_of_category,
        unit_cost_sum_2_prior,
        RANK() OVER (ORDER BY category_max_unit_cost NULLS LAST, id) AS category_max_unit_cost_rank,
        MAX(unit_cost) OVER (ORDER BY unit_cost_share_of_category DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS unit_cost_max_3_prior,
        NTILE(4) OVER (ORDER BY category_max_unit_cost NULLS LAST, id) AS category_max_unit_cost_quartile,
        FIRST_VALUE(last_unit_cost) IGNORE NULLS OVER (ORDER BY unit_cost_share_of_category DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_unit_cost,
        ROW_NUMBER() OVER (ORDER BY unit_cost_share_of_category DESC, id) AS seq_no,
        LEAD(unit_cost_sum_2_prior, 1, 0) OVER (ORDER BY unit_cost_share_of_category DESC, id) AS next_unit_cost_sum_2_prior,
        LAST_VALUE(weight_kg_avg_3_prior) OVER (ORDER BY unit_cost_share_of_category DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_weight_kg_avg_3_prior
    FROM derived
)
-- Final projection
SELECT
    id,
    unit_cost,
    unit_cost_share_of_category,
    unit_cost_sum_2_prior,
    category_max_unit_cost_rank,
    unit_cost_max_3_prior,
    category_max_unit_cost_quartile,
    first_known_last_unit_cost,
    seq_no,
    ROUND(unit_cost_max_3_prior / NULLIF(seq_no, 0), 3) AS unit_cost_max_3_prior_per_seq_no,
    ROUND(unit_cost / NULLIF(seq_no, 0), 3) AS unit_cost_per_seq_no
FROM with_trends
WHERE category_max_unit_cost_quartile <= 8
ORDER BY category_max_unit_cost_quartile DESC NULLS FIRST, id
LIMIT 10

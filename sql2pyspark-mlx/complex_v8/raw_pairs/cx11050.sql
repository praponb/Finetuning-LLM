WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        SUBSTRING(TRIM(sku_code), 1, 3) AS sku_code_prefix
    FROM skus
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_is_active_category AS rows_in_is_active_category,
        weight_kg - LAG(weight_kg) OVER (PARTITION BY category, is_active ORDER BY unit_cost, id) AS weight_kg_change,
        ROUND(unit_cost - AVG(unit_cost) OVER (PARTITION BY category), 2) AS unit_cost_vs_category_avg,
        NTH_VALUE(weight_kg, 2) OVER (ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg,
        ROUND(AVG(weight_kg) OVER (ORDER BY weight_kg, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS weight_kg_avg_7,
        ROUND(unit_cost / NULLIF(SUM(unit_cost) OVER (PARTITION BY category), 0), 4) AS unit_cost_share_of_category,
        NTILE(4) OVER w_category_is_active_by_unit_c AS unit_cost_quartile,
        ROW_NUMBER() OVER (PARTITION BY is_active, category ORDER BY weight_kg DESC NULLS FIRST, id) AS seq_no,
        LAST_VALUE(unit_cost) OVER (PARTITION BY is_active, category ORDER BY weight_kg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        MIN(unit_cost) OVER w_category AS category_min_unit_cost
    FROM cleaned_skus
    WINDOW w_is_active_category AS (PARTITION BY is_active, category), w_category_is_active_by_unit_c AS (PARTITION BY category, is_active ORDER BY unit_cost, id), w_category AS (PARTITION BY category)
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY seq_no ASC) AS seq_no_dense_rank,
        LAG(weight_kg_avg_7) OVER w_category_by_seq_no AS prev_weight_kg_avg_7,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_cost_share_of_category DESC, id) AS rn,
        RANK() OVER (PARTITION BY category ORDER BY seq_no ASC, id) AS seq_no_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY category ORDER BY seq_no ASC), 3) AS seq_no_percent_rank,
        RANK() OVER (PARTITION BY seq_no, unit_cost_quartile ORDER BY unit_cost_quartile DESC) AS unit_cost_quartile_rank,
        SUM(unit_cost_share_of_category) OVER (PARTITION BY seq_no ORDER BY weight_kg_avg_7 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_cost_share_of_category
    FROM with_trends
    WINDOW w_category_by_seq_no AS (PARTITION BY category ORDER BY seq_no ASC, id)
)
SELECT
    id,
    sku_code,
    category,
    unit_cost,
    weight_kg,
    labels,
    is_active,
    rows_in_is_active_category,
    weight_kg_change,
    unit_cost_vs_category_avg,
    second_weight_kg,
    weight_kg_avg_7,
    unit_cost_share_of_category,
    unit_cost_quartile,
    seq_no,
    last_unit_cost,
    category_min_unit_cost,
    seq_no_dense_rank,
    prev_weight_kg_avg_7,
    seq_no_rank,
    seq_no_percent_rank,
    unit_cost_quartile_rank
FROM with_trends_2
ORDER BY prev_weight_kg_avg_7 DESC NULLS FIRST

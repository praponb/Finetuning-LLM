WITH base_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        weight_kg,
        is_active,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        UPPER(TRIM(sku_code)) AS sku_code_clean
    FROM skus
    WHERE SIZE(labels) >= 2
      AND unit_cost <= 330.6
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        sku_code,
        weight_kg,
        sku_code_ref,
        sku_code_clean,
        ROUND(weight_kg / NULLIF(SUM(weight_kg) OVER w_is_active, 0), 4) AS weight_kg_share_of_is_active,
        ROUND(weight_kg - AVG(weight_kg) OVER w_is_active, 2) AS weight_kg_vs_is_active_avg,
        FIRST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY is_active ORDER BY weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg,
        ROUND(AVG(weight_kg) OVER (PARTITION BY is_active ORDER BY weight_kg DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS weight_kg_avg_4,
        LAG(weight_kg) OVER w_is_active_by_weight_kg AS prev_weight_kg,
        NTH_VALUE(weight_kg, 2) OVER (ORDER BY weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_weight_kg,
        LAG(weight_kg, 1, 0) OVER w_is_active_by_weight_kg_2 AS prev_weight_kg_2,
        ROW_NUMBER() OVER w_is_active_by_weight_kg_2 AS seq_no,
        COUNT(*) OVER (PARTITION BY is_active) AS rows_in_is_active,
        SUM(weight_kg) OVER (PARTITION BY is_active) AS is_active_sum_weight_kg
    FROM base_skus
    WINDOW w_is_active AS (PARTITION BY is_active), w_is_active_by_weight_kg AS (PARTITION BY is_active ORDER BY weight_kg NULLS LAST, id), w_is_active_by_weight_kg_2 AS (PARTITION BY is_active ORDER BY weight_kg DESC, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        sku_code,
        weight_kg,
        sku_code_ref,
        sku_code_clean,
        weight_kg_share_of_is_active,
        weight_kg_vs_is_active_avg,
        first_known_weight_kg,
        weight_kg_avg_4,
        prev_weight_kg,
        second_weight_kg,
        prev_weight_kg_2,
        rows_in_is_active,
        is_active_sum_weight_kg
    FROM ranked
    WHERE seq_no <= 5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY weight_kg DESC) AS weight_kg_dense_rank,
        NTILE(5) OVER (ORDER BY first_known_weight_kg, id) AS first_known_weight_kg_quintile,
        ROUND(CUME_DIST() OVER w_all_by_weight_kg, 3) AS weight_kg_cume_dist
    FROM top_rows
    WINDOW w_all_by_weight_kg AS (ORDER BY weight_kg DESC, id)
)
-- Final projection
SELECT
    id,
    sku_code,
    weight_kg,
    sku_code_ref,
    sku_code_clean,
    weight_kg_vs_is_active_avg,
    first_known_weight_kg,
    weight_kg_avg_4,
    prev_weight_kg,
    prev_weight_kg_2,
    rows_in_is_active,
    is_active_sum_weight_kg,
    first_known_weight_kg_quintile,
    weight_kg_cume_dist
FROM windowed
ORDER BY weight_kg_cume_dist

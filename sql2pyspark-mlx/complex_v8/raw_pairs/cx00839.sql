WITH filtered AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        LENGTH(TRIM(sku_code)) AS sku_code_len
    FROM skus
    WHERE is_active = FALSE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(unit_cost) OVER (PARTITION BY sku_code_len) AS sku_code_len_min_unit_cost,
        ROUND(AVG(unit_cost) OVER (PARTITION BY sku_code_len), 2) AS sku_code_len_avg_unit_cost,
        LAST_VALUE(sku_code_len) IGNORE NULLS OVER (ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_code_len,
        LAG(sku_code_len, 2, 0) OVER w_sku_code_len_by_sku_code_len AS prev2_sku_code_len,
        NTH_VALUE(sku_code_len, 2) OVER (PARTITION BY sku_code_len ORDER BY sku_code_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_code_len,
        COUNT(*) OVER (PARTITION BY sku_code_len) AS rows_in_sku_code_len,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY sku_code_len ORDER BY weight_kg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        LAST_VALUE(weight_kg) OVER (PARTITION BY sku_code_len ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_weight_kg,
        weight_kg - LAG(weight_kg) OVER w_all_by_unit_cost AS weight_kg_change,
        MIN(sku_code_len) OVER w_sku_code_len AS sku_code_len_min_sku_code_len
    FROM filtered
    WINDOW w_sku_code_len_by_sku_code_len AS (PARTITION BY sku_code_len ORDER BY sku_code_len ASC, id), w_all_by_unit_cost AS (ORDER BY unit_cost, id), w_sku_code_len AS (PARTITION BY sku_code_len)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(sku_code_ref, '#', CAST(prev2_sku_code_len AS STRING)) AS sku_code_ref_ref
    FROM with_trends
    WHERE sku_code_len_avg_unit_cost < 320.2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        weight_kg,
        sku_code_ref,
        sku_code_len_min_unit_cost,
        last_known_sku_code_len,
        prev2_sku_code_len,
        second_sku_code_len,
        last_known_unit_cost,
        last_weight_kg,
        weight_kg_change,
        sku_code_len_min_sku_code_len,
        ROUND(last_known_unit_cost - AVG(last_known_unit_cost) OVER (PARTITION BY sku_code_len), 2) AS last_known_unit_cost_vs_sku_code_len_avg,
        prev2_sku_code_len - LAG(prev2_sku_code_len) OVER w_all_by_rows_in_sku_code_len AS prev2_sku_code_len_change,
        ROW_NUMBER() OVER w_sku_code_len_by_last_weight AS rn
    FROM labeled
    WINDOW w_all_by_rows_in_sku_code_len AS (ORDER BY rows_in_sku_code_len DESC, id), w_sku_code_len_by_last_weight AS (PARTITION BY sku_code_len ORDER BY last_weight_kg DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    sku_code,
    weight_kg,
    sku_code_ref,
    last_known_sku_code_len,
    prev2_sku_code_len,
    second_sku_code_len,
    weight_kg_change,
    sku_code_len_min_sku_code_len,
    last_known_unit_cost_vs_sku_code_len_avg,
    prev2_sku_code_len_change,
    CASE WHEN second_sku_code_len >= 9 THEN 'HIGH' WHEN second_sku_code_len >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS second_sku_code_len_tier,
    LENGTH(TRIM(sku_code)) AS sku_code_len
FROM ranked
WHERE sku_code_len_min_unit_cost > 400.9
ORDER BY sku_code_len_min_sku_code_len

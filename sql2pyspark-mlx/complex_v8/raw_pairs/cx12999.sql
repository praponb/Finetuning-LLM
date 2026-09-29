WITH prepared AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        ABS(unit_cost - 231.6) AS unit_cost_distance,
        ARRAY_CONTAINS(labels, 'bulky') AS has_bulky,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM skus
    WHERE is_active = FALSE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        is_active,
        unit_cost_distance,
        has_bulky,
        is_active_label,
        FIRST_VALUE(unit_cost) OVER (PARTITION BY is_active, is_active_label ORDER BY weight_kg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_cost,
        COUNT(*) OVER (PARTITION BY category, is_active) AS rows_in_category_is_active
    FROM prepared
),
latest AS (
    -- Keep the earliest row per is_active_label
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        is_active,
        unit_cost_distance,
        has_bulky,
        is_active_label,
        first_unit_cost,
        rows_in_category_is_active
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY is_active_label ORDER BY first_unit_cost, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        is_active_label,
        SUM(rows_in_category_is_active) FILTER (WHERE LOWER(sku_code) LIKE '%el%') AS rows_in_category_is_active_lower_sku_code_like_e,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(first_unit_cost), 2) AS total_first_unit_cost,
        COUNT(*) FILTER (WHERE is_active = FALSE) AS n_is_active_false
    FROM latest
    GROUP BY is_active_label
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE rows_in_category_is_active_lower_sku_code_like_e > (SELECT AVG(rows_in_category_is_active_lower_sku_code_like_e) FROM totals)
),
enriched AS (
    -- Derive helper columns
    SELECT
        is_active_label,
        rows_in_category_is_active_lower_sku_code_like_e,
        total_first_unit_cost,
        n_is_active_false,
        LENGTH(TRIM(is_active_label)) AS is_active_label_len,
        distinct_ids - total_first_unit_cost AS distinct_ids_minus_total_first_unit_cost
    FROM without_matches
),
without_matches_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE total_first_unit_cost > (SELECT MIN(total_first_unit_cost) FROM enriched)
)
-- Final projection
SELECT
    is_active_label,
    rows_in_category_is_active_lower_sku_code_like_e,
    total_first_unit_cost,
    n_is_active_false,
    SUBSTRING(TRIM(is_active_label), 1, 3) AS is_active_label_prefix,
    ABS(n_is_active_false - 6) AS n_is_active_false_distance
FROM without_matches_2
WHERE is_active_label_len >= 4
ORDER BY rows_in_category_is_active_lower_sku_code_like_e DESC, n_is_active_false DESC

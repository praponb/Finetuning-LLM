WITH filtered_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        CASE WHEN category IN ('Beauty', 'Food') THEN 'group_a' WHEN category = 'Toys' THEN 'group_b' ELSE 'other' END AS category_group,
        CONCAT_WS(' / ', category, sku_code) AS category_sku_code_label
    FROM skus
    WHERE weight_kg > 6.4
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        category_group,
        category_sku_code_label,
        LENGTH(TRIM(category_group)) AS category_group_len
    FROM filtered_skus
    WHERE is_active = TRUE
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        category_group,
        category_group_len,
        ROW_NUMBER() OVER (ORDER BY weight_kg ASC, id) AS weight_kg_seq,
        SUM(category_group_len) OVER w_category_group_len AS category_group_len_sum_category_group_len
    FROM enriched
    WINDOW w_category_group_len AS (PARTITION BY category_group_len)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        category_group_len,
        weight_kg_seq,
        category_group_len_sum_category_group_len,
        weight_kg - category_group_len AS weight_kg_minus_category_group_len,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM sequenced
    WHERE weight_kg < 18.5
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        category_or_unknown,
        category,
        category_group_len,
        COUNT(*) FILTER (WHERE UPPER(TRIM(sku_code)) LIKE 'A%') AS n_upper_trim_sku_code_like_a,
        SUM(CASE WHEN UPPER(TRIM(sku_code)) LIKE 'T%' THEN 1 ELSE 0 END) AS n_upper_trim_sku_code_like_t,
        PERCENTILE_APPROX(weight_kg_minus_category_group_len, 0.5) AS median_weight_kg_minus_category_group_len
    FROM scored
    GROUP BY 1, 2, 3
    HAVING SUM(category_group_len_sum_category_group_len) > 16
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        category_or_unknown,
        category,
        category_group_len,
        n_upper_trim_sku_code_like_t,
        median_weight_kg_minus_category_group_len,
        LENGTH(TRIM(category)) AS category_len
    FROM totals
    WHERE n_upper_trim_sku_code_like_a BETWEEN 2 AND 8
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(category_len, category_group_len) AS greatest_category_len_category_group_len,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        ROUND(category_group_len / NULLIF(category_len, 0), 3) AS category_group_len_per_category_len
    FROM enriched_2
)
-- Final projection
SELECT
    category_or_unknown,
    category,
    category_group_len,
    median_weight_kg_minus_category_group_len,
    category_len,
    greatest_category_len_category_group_len,
    category_slug,
    category_group_len_per_category_len
FROM labeled
WHERE category IN ('Food', 'Beauty', 'Electronics')
ORDER BY category_group_len_per_category_len DESC NULLS FIRST, greatest_category_len_category_group_len DESC

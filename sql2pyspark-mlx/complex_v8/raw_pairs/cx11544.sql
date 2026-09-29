WITH prepared_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        labels,
        UPPER(TRIM(sku_code)) AS sku_code_clean,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM skus
    WHERE (category IN ('Beauty', 'Food') OR unit_cost > 38.3)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(category, '#', CAST(id AS STRING)) AS category_ref,
        LENGTH(TRIM(category)) AS category_len,
        LENGTH(TRIM(category)) AS category_len_2
    FROM prepared_skus
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_active_label,
        category_len,
        category,
        COUNT(*) AS cnt,
        ROUND(SUM(category_len_2) / NULLIF(SUM(category_len), 0), 3) AS category_len_2_to_category_len_ratio
    FROM tagged
    GROUP BY is_active_label, category_len, category
)
SELECT *
FROM (
    SELECT category_len, category, cnt
    FROM rollup_base
) src
PIVOT (
    AVG(cnt)
    FOR category IN ('Toys', 'Apparel', 'Beauty', 'Food')
)
ORDER BY category_len

WITH cleaned_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        is_active
    FROM skus
),
unique_rows AS (
    -- Keep the earliest row per category
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        is_active
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY unit_cost, id) AS recency_rank
        FROM cleaned_skus
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        is_active,
        ROUND(100 * COUNT(CASE WHEN is_active = FALSE THEN 1 END) / COUNT(*), 1) AS pct_is_active_false,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list
    FROM unique_rows
    GROUP BY is_active
),
enriched AS (
    -- Label and score rows
    SELECT
        is_active,
        pct_is_active_false,
        n_rows,
        distinct_ids,
        category_list,
        CASE WHEN n_rows >= 7 THEN 'large' WHEN n_rows >= 4 THEN 'medium' ELSE 'small' END AS n_rows_tier
    FROM grouped
    WHERE pct_is_active_false > 35.0
),
deduped AS (
    -- Keep the latest row per n_rows_tier
    SELECT
        is_active,
        pct_is_active_false,
        n_rows,
        distinct_ids,
        category_list,
        n_rows_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY n_rows_tier ORDER BY n_rows DESC, is_active DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
)
SELECT *
FROM (
    SELECT n_rows, is_active, n_rows_tier, pct_is_active_false
    FROM deduped
) src
PIVOT (
    MAX(pct_is_active_false)
    FOR n_rows_tier IN ('large', 'small', 'medium')
)
ORDER BY n_rows, is_active

WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        LOWER(TRIM(product)) AS product_clean,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
    WHERE (holder_id IS NOT NULL OR premium <= 3521.8)
),
latest_per_key AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        product_clean,
        premium_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(product, 'ALL') AS product,
    holder_id,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM latest_per_key
GROUP BY GROUPING SETS ((status, product, holder_id), (status))
ORDER BY status NULLS LAST, product NULLS LAST, holder_id NULLS LAST

WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean,
        LOWER(TRIM(product)) AS product_clean,
        INITCAP(TRIM(status)) AS status_clean_2
    FROM policies
    WHERE (end_date IS NOT NULL OR holder_id <= 14)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        product,
        premium,
        end_date,
        status,
        product_clean,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        ABS(premium - 2033.8) AS premium_distance,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM valid
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status_or_unknown), ' ', '_')) AS status_or_unknown_slug,
        COALESCE(days_since_end_date, 0) AS days_since_end_date_filled,
        ROUND(premium / NULLIF(premium_distance, 0), 3) AS premium_per_premium_distance
    FROM annotated
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        end_date,
        status,
        premium_distance,
        status_or_unknown_slug,
        premium_per_premium_distance,
        ROUND(premium_distance * 0.9, 2) AS premium_distance_adj,
        ROUND(premium / NULLIF(days_since_end_date, 0), 3) AS premium_per_days_since_end_date
    FROM tagged
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM scored
GROUP BY ROLLUP(product, status)
ORDER BY product NULLS LAST, status NULLS LAST

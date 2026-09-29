WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE (YEAR(start_date) = 2024 OR YEAR(end_date) = 2026)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        product,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(*) AS row_count,
        SUM(CASE WHEN product = 'TRAVEL' THEN premium ELSE 0 END) AS premium_product_travel
    FROM cleaned
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), product, DATE_TRUNC('MONTH', end_date)
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    start_date_ym,
    product,
    product_list,
    premium_product_travel
FROM per_group
ORDER BY start_date_ym DESC, premium_product_travel

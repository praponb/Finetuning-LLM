WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status = 'EXPIRED'
      AND start_date < DATE '2025-02-13'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        TRUNC(end_date, 'MM') AS end_date_month_start,
        LOWER(TRIM(product)) AS product_clean,
        CASE WHEN premium >= 3544.1 THEN 'A' WHEN premium >= 2289.0 THEN 'B' ELSE 'C' END AS premium_tier
    FROM base
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        YEAR(start_date) AS start_date_year,
        COUNT(*) AS row_count,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        MIN(end_date) AS first_end_date,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM labeled
    GROUP BY holder_id, YEAR(start_date)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE start_date_year < (SELECT MAX(start_date_year) FROM stats)
)
SELECT
    holder_id,
    start_date_year,
    stddev_premium,
    first_end_date,
    product_list,
    status_list,
    COALESCE(start_date_year, 0) AS start_date_year_filled
FROM without_matches
ORDER BY stddev_premium DESC, start_date_year NULLS LAST, holder_id
LIMIT 5

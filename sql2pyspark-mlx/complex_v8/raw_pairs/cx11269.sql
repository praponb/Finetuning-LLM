WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE status <> 'LAPSED'
),
stats AS (
    -- Summarise per group
    SELECT
        product,
        YEAR(start_date) AS start_date_year,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        MIN(premium) AS min_premium,
        SUM(premium) AS total_premium
    FROM filtered_policys
    WHERE premium <= 2945.9
    GROUP BY 1, 2, 3
    HAVING SUM(premium) > 1132.1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        product,
        start_date_year,
        end_date_month,
        median_premium,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month) ORDER BY min_premium DESC, product, start_date_year, end_date_month) AS seq_no,
        FIRST_VALUE(end_date_month) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month) ORDER BY min_premium DESC, product, start_date_year, end_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date_month,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month) ORDER BY min_premium DESC, product, start_date_year, end_date_month) AS min_premium_tercile
    FROM stats
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(product)) AS product_len,
        ROUND(start_date_year / NULLIF(median_premium, 0), 3) AS start_date_year_per_median_premium,
        DATE_TRUNC('YEAR', first_end_date_month) AS first_end_date_month_year
    FROM with_trends
)
-- Final projection
SELECT
    product,
    start_date_year,
    seq_no,
    first_end_date_month,
    min_premium_tercile,
    first_end_date_month_year,
    ROUND(seq_no / NULLIF(product_len, 0), 3) AS seq_no_per_product_len
FROM annotated
ORDER BY first_end_date_month_year DESC

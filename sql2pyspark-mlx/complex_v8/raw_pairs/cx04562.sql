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
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM policies
    WHERE product <> 'HEALTH'
      AND holder_id >= 18
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date_week) AS end_date_week_month,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        MIN(premium) AS min_premium,
        ROUND(SUM(premium_bucket) / NULLIF(SUM(premium), 0), 3) AS premium_bucket_to_premium_ratio,
        ROUND(SUM(premium), 2) AS total_premium
    FROM valid
    WHERE product IN ('HEALTH', 'LIFE')
    GROUP BY DATE_TRUNC('MONTH', end_date_week), DATE_FORMAT(end_date, 'yyyy-MM')
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE total_premium > (SELECT MIN(total_premium) FROM grouped)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(end_date_week_month, 'yyyy-MM-dd') AS end_date_week_month_label,
        GREATEST(min_premium, total_premium) AS greatest_min_premium_total_premium,
        UPPER(TRIM(end_date_ym)) AS end_date_ym_clean
    FROM filtered
)
-- Native SQL UNPIVOT (wide to long)
SELECT end_date_week_month, end_date_ym, measure, reading
FROM (
    SELECT end_date_week_month, end_date_ym, premium_bucket_to_premium_ratio, greatest_min_premium_total_premium, min_premium, total_premium
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (premium_bucket_to_premium_ratio, greatest_min_premium_total_premium, min_premium, total_premium)
) unp

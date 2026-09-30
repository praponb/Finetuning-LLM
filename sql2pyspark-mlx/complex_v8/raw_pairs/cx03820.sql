WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
),
grouped AS (
    -- Summarise per group
    SELECT
        status,
        COUNT(*) AS n_rows,
        MAX(days_since_end_date) AS max_days_since_end_date,
        ROUND(SUM(days_since_end_date) / NULLIF(SUM(premium), 0), 3) AS days_since_end_date_to_premium_ratio,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT product) AS distinct_products
    FROM filtered
    WHERE product = 'HOME'
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE n_rows > (SELECT MIN(n_rows) FROM grouped)
)
SELECT status, field, metric_value
FROM (
    SELECT status, distinct_products, distinct_ids
    FROM with_activity
) src
UNPIVOT (
    metric_value FOR field IN (distinct_products, distinct_ids)
) unp

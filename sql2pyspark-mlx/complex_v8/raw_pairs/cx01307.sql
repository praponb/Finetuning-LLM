WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE end_date BETWEEN DATE '2024-11-08' AND DATE '2026-11-25'
      AND status IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        holder_id,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        MAX(start_date) AS last_start_date
    FROM cleaned
    GROUP BY 1, 2, 3
),
with_details AS (
    -- Join policyholders
    SELECT s.*, p.holder_name, p.risk_score AS holder_risk_score
    FROM per_group s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    end_date_month,
    start_date_month,
    holder_id,
    median_premium,
    last_start_date,
    holder_name,
    holder_risk_score,
    ABS(median_premium - 2120.0) AS median_premium_distance
FROM with_details
ORDER BY holder_name, end_date_month, start_date_month, holder_id
LIMIT 5

WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE status = 'ACTIVE'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        YEAR(start_date) AS start_date_year,
        holder_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(start_date) AS first_start_date,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        MAX(premium) AS max_premium,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM scoped
    GROUP BY DATE_TRUNC('MONTH', end_date), YEAR(start_date), holder_id
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    end_date_month,
    start_date_year,
    holder_id,
    cnt,
    distinct_ids,
    first_start_date,
    end_date_span_days,
    max_premium,
    ROUND(max_premium * 1.1, 2) AS max_premium_adj,
    CASE WHEN max_premium >= 3080.0 THEN 'A' WHEN max_premium >= 1880.0 THEN 'B' ELSE 'C' END AS max_premium_tier
FROM rollup_base
ORDER BY max_premium_tier DESC

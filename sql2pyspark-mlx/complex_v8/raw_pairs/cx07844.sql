WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_clean,
        CASE WHEN premium >= 3532.7 THEN 'A' WHEN premium >= 2289.0 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE product IS NOT NULL
      AND end_date >= DATE '2026-02-26'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT(premium_tier, '#', CAST(holder_id AS STRING)) AS premium_tier_ref
    FROM cleaned_policys
    WHERE holder_id >= 6
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        premium_filled,
        ROUND(premium_filled / NULLIF(premium, 0), 3) AS premium_filled_per_premium,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT(premium_tier_ref, '#', CAST(holder_id AS STRING)) AS premium_tier_ref_ref
    FROM tagged
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        premium_filled,
        premium_filled_per_premium,
        ROUND(premium_filled_per_premium - AVG(premium_filled_per_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status), 2) AS premium_filled_per_premium_vs_start_date_month_s,
        premium_filled_per_premium - LAG(premium_filled_per_premium) OVER (PARTITION BY holder_id ORDER BY premium_filled_per_premium, id) AS premium_filled_per_premium_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY premium_filled_per_premium, id), 3) AS premium_filled_per_premium_percent_rank
    FROM enriched
)
SELECT *
FROM (
    SELECT end_date, status, premium
    FROM metrics
) src
PIVOT (
    COUNT(premium)
    FOR status IN ('ACTIVE', 'CANCELLED')
)
ORDER BY end_date

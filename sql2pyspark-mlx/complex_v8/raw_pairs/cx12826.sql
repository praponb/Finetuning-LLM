WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE product <> 'AUTO'
      AND end_date BETWEEN DATE '2025-04-17' AND DATE '2027-01-03'
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM cleaned
    GROUP BY status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM cleaned s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.premium_filled,
        s.status_slug,
        s.days_since_end_date,
        s.avg_premium,
        s.premium_minus_avg,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM compared s
    JOIN policyholders p ON p.id = s.holder_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.segment AS holder_segment
    FROM with_parent s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment = 'SME'
)
SELECT id, metric, metric_value
FROM (
    SELECT id, premium_filled, premium, premium_minus_avg
    FROM with_details
) src
UNPIVOT (
    metric_value FOR metric IN (premium_filled, premium, premium_minus_avg)
) unp
ORDER BY id, metric

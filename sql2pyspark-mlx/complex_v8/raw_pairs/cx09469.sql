WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        region,
        risk_score,
        segment,
        DATE_TRUNC('YEAR', joined_on) AS joined_on_year,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE LOWER(holder_name) LIKE '%le%'
      AND risk_score >= 0.1
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', joined_on_year) AS days_since_joined_on_year
    FROM cleaned
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count
    FROM scored s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT joined_on_year, region, policy_count
    FROM with_stats
) src
PIVOT (
    MIN(policy_count)
    FOR region IN ('Central', 'North', 'East')
)

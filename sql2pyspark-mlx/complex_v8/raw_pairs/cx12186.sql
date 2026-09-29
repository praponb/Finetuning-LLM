WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE adjuster_id >= 7
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM base_claims
    WHERE days_since_reported_at > (SELECT AVG(days_since_reported_at) FROM base_claims)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_type, claim_date, status, days_since_reported_at
    FROM screened
) src
PIVOT (
    SUM(days_since_reported_at)
    FOR status IN ('IN_REVIEW', 'APPROVED', 'REJECTED', 'OPEN')
)

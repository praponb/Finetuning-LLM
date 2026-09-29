WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-02-19' AND DATE '2025-05-26'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        MONTH(claim_date) AS claim_date_month
    FROM prepared_claims
),
screened AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product = 'TRAVEL'
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        claimed_amount,
        reported_at,
        status_or_unknown,
        claim_type_clean,
        days_since_reported_at,
        ABS(days_since_reported_at - 315) AS days_since_reported_at_distance
    FROM screened
)
-- Final projection
SELECT
    id,
    claim_date,
    reported_at,
    status_or_unknown,
    days_since_reported_at,
    days_since_reported_at_distance,
    ABS(claimed_amount - 29985.9) AS claimed_amount_distance,
    ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
FROM annotated
WHERE days_since_reported_at > 105
ORDER BY days_since_reported_at_distance

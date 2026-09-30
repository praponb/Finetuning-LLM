WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id <= 2
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date
    FROM recent_claims s
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        reported_at,
        claim_count,
        last_claim_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM profiled
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type IS NOT NULL
              AND c.reported_at < TIMESTAMP '2025-02-18 15:00:00'
        )
)
SELECT
    id,
    adjuster_id,
    reported_at,
    claim_count,
    last_claim_date,
    status_or_unknown,
    reported_at_plus_2h,
    LOWER(REGEXP_REPLACE(TRIM(status_or_unknown), ' ', '_')) AS status_or_unknown_slug,
    COALESCE(status_or_unknown, 'UNKNOWN') AS status_or_unknown_or_unknown
FROM with_activity

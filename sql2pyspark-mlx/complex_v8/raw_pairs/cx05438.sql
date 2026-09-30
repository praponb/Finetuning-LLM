WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE (adjuster_id IS NOT NULL OR policy_id IS NOT NULL)
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at
    FROM recent_claims s
),
deduped AS (
    -- Keep the latest row per adjuster_id, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        days_since_claim_date,
        claim_type_slug,
        first_claim_reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, claim_type ORDER BY claim_date DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_date, status, days_since_claim_date
    FROM deduped
) src
PIVOT (
    SUM(days_since_claim_date)
    FOR status IN ('APPROVED', 'REJECTED')
)
ORDER BY claim_date

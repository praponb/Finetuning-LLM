WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claim_type IS NOT NULL
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.status_prefix,
        c.id AS claim_id,
        c.claim_type,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date
    FROM filtered_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_children_claims
    WHERE claimed_amount > (SELECT MIN(claimed_amount) FROM with_children_claims)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        claim_date,
        claimed_amount,
        reported_at,
        status_prefix,
        claim_id,
        claim_type,
        claim_reported_at,
        claim_claim_date,
        LOWER(REGEXP_REPLACE(TRIM(status_prefix), ' ', '_')) AS status_prefix_slug,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        LENGTH(TRIM(status)) AS status_len
    FROM without_matches
)
SELECT
    claim_id,
    claim_type,
    COUNT(*) AS row_count
FROM enriched
GROUP BY GROUPING SETS ((claim_id, claim_type), (claim_id), ())

WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM claims
    WHERE claimed_amount <= 40639.7
),
latest_per_key AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claimed_amount_plus_approved_amount,
        claim_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id DESC) AS rn
        FROM prepared_claims
    ) ranked
    WHERE rn = 1
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_prefix,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status,
        c.claim_date AS claim_claim_date
    FROM latest_per_key s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.status IN ('IN_REVIEW', 'APPROVED', 'REJECTED')
)
SELECT
    claim_policy_id,
    adjuster_id,
    GROUPING_ID() AS grouping_level,
    SUM(approved_amount) FILTER (WHERE claim_status = 'OPEN') AS approved_amount_claim_status_open
FROM with_children
GROUP BY GROUPING SETS ((claim_policy_id, adjuster_id), (claim_policy_id), ())
ORDER BY claim_policy_id NULLS LAST, adjuster_id NULLS LAST

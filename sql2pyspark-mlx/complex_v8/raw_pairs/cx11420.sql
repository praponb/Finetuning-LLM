WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE YEAR(claim_date) = 2025
      AND status = 'REJECTED'
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.status_ref,
        a.level AS adjuster_level
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.status_ref,
        s.adjuster_level,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM with_details s
    LEFT JOIN policies p ON p.id = s.policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT COUNT(*) FILTER (WHERE c.status IN ('OPEN', 'APPROVED')) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM with_parent_policy s
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status_ref,
        s.policy_premium,
        s.claim_status_count,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type,
        c.claim_date,
        c.reported_at AS claim_reported_at
    FROM summarized_rows s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type IN ('delay', 'theft')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claim_claim_type, claim_id, claim_type, policy_premium
    FROM exploded_detail_claims
) src
PIVOT (
    AVG(policy_premium)
    FOR claim_type IN ('fire', 'collision', 'theft')
)

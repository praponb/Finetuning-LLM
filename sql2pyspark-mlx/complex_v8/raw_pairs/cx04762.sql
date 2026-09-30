WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek,
        CASE WHEN status IN ('IN_REVIEW', 'OPEN') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claimed_amount > 19337.6
      AND status = 'APPROVED'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.days_since_claim_date,
        s.status_group,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.policy_id AS claim_policy_id
    FROM valid_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
enriched_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status_group,
        s.claim_id,
        s.claim_reported_at,
        s.claim_status,
        s.claim_claimed_amount,
        s.claim_policy_id,
        c.claimed_amount AS claim_claimed_amount_2
    FROM exploded_detail_claims s
    JOIN claims c ON c.id = s.claim_id
)
SELECT
    COALESCE(claim_status, 'ALL') AS claim_status,
    claim_policy_id,
    policy_id,
    GROUPING(claim_status) AS is_total_claim_status,
    ROUND(SUM(claim_claimed_amount_2) / NULLIF(SUM(approved_amount), 0), 3) AS claim_claimed_amount_2_to_approved_amount_ratio,
    COUNT(DISTINCT claim_id) AS distinct_claim_ids,
    MAX(claim_reported_at) AS last_claim_reported_at
FROM enriched_claim
GROUP BY GROUPING SETS ((claim_status, claim_policy_id, policy_id), (claim_status), (claim_policy_id), ())

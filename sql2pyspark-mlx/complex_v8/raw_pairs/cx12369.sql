WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM joined_ranges s
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.product,
        s.claim_count,
        c.id AS claim_id,
        c.claim_type,
        c.claim_date AS claim_claim_date,
        c.status AS claim_status
    FROM profiled s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    claimed_amount,
    product,
    claim_count,
    claim_id,
    claim_type,
    claim_claim_date,
    COALESCE(claim_count, 0) AS claim_count_filled,
    FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
FROM with_children_claims
ORDER BY claim_type DESC, claim_id DESC

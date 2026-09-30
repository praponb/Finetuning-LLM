WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.claim_type,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM joined_ranges s
    JOIN claims c ON c.policy_id = s.policy_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(claimed_amount) AS total_claim_claimed_amount,
        COUNT(*) AS claim_count,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.max_claim_approved_amount
    FROM exploded_detail s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT
    id,
    policy_id,
    claim_date,
    status,
    claim_type,
    claim_status,
    claim_reported_at,
    total_claim_claimed_amount,
    claim_count,
    max_claim_approved_amount
FROM with_counts
ORDER BY total_claim_claimed_amount DESC NULLS FIRST

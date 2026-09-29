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
        reported_at
    FROM claims
    WHERE adjuster_id <= 3
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claim_date) AS last_claim_date,
        MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.min_claim_approved_amount
    FROM prepared_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT
    policy_id,
    adjuster_id,
    claim_type,
    ROUND(SUM(approved_amount), 2) AS total_approved_amount,
    ROUND(SUM(min_claim_approved_amount) / NULLIF(SUM(approved_amount), 0), 3) AS min_claim_approved_amount_to_approved_amount_rat,
    COUNT(*) AS cnt
FROM with_stats
GROUP BY CUBE(policy_id, adjuster_id, claim_type)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST, claim_type NULLS LAST

WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id >= 2
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM cleaned s
    JOIN claims c ON c.policy_id = s.policy_id AND c.approved_amount > 0.0
)
SELECT
    claim_status,
    adjuster_id,
    GROUPING(claim_status) AS is_total_claim_status,
    COUNT(*) AS row_count
FROM exploded_detail
GROUP BY ROLLUP(claim_status, adjuster_id)
ORDER BY claim_status NULLS LAST, adjuster_id NULLS LAST

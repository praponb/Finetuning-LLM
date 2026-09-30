WITH base AS (
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
    WHERE claim_date < DATE '2025-05-30'
),
joined_policy AS (
    -- Enrich with dimension data
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
        p.status AS policy_status
    FROM base s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.status = 'ACTIVE'
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.policy_status,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM joined_policy s
    JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with CUBE
SELECT
    policy_status,
    adjuster_id,
    policy_id,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(policy_premium), 2) AS stddev_policy_premium
FROM joined
GROUP BY CUBE(policy_status, adjuster_id, policy_id)
ORDER BY policy_status NULLS LAST, adjuster_id NULLS LAST, policy_id NULLS LAST

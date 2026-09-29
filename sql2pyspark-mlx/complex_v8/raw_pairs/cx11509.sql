WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_type = 'medical'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_approved_amount
    FROM recent_claims s
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_count,
        s.last_claim_reported_at,
        s.avg_claim_approved_amount,
        p.holder_id AS policy_holder_id
    FROM profiled s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.premium < 3719.7
)
SELECT
    policy_id,
    status,
    ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount,
    MAX(last_claim_reported_at) AS last_last_claim_reported_at,
    ROUND(AVG(claim_count), 2) AS avg_claim_count
FROM joined_policy
GROUP BY GROUPING SETS ((policy_id, status), (policy_id), (status))
ORDER BY policy_id NULLS LAST, status NULLS LAST

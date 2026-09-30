WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status
    FROM claims
    WHERE policy_id >= 16
      AND claim_type IN ('flood', 'fire', 'delay')
),
filtered_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM filtered_2 s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.status = 'ACTIVE'
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(CASE WHEN status IN ('IN_REVIEW', 'REJECTED') THEN 1 END) AS claim_status_count,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.first_claim_date
    FROM with_details_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT id, field, metric_value
FROM (
    SELECT id, policy_premium, claimed_amount
    FROM profiled
) src
UNPIVOT (
    metric_value FOR field IN (policy_premium, claimed_amount)
) unp
ORDER BY id, field

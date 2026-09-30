WITH scoped_claims AS (
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
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE (claim_type = 'flood' OR status NOT IN ('OPEN', 'PAID'))
),
enriched_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.premium AS policy_premium
    FROM scoped_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
adjuster_id_stats AS (
    -- Median policy_premium per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(policy_premium, 0.5) AS median_policy_premium
    FROM enriched_adjuster
    GROUP BY adjuster_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_policy_premium,
        ROUND(s.policy_premium - g.median_policy_premium, 2) AS policy_premium_minus_median
    FROM enriched_adjuster s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.policy_premium >= g.median_policy_premium
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.least_approved_amount_claimed_amount,
        s.adjuster_level,
        s.adjuster_name,
        s.adjuster_team,
        s.policy_premium,
        p.premium AS policy_premium_2,
        p.holder_id AS policy_holder_id
    FROM vs_group s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    status,
    least_approved_amount_claimed_amount,
    adjuster_name,
    adjuster_team,
    policy_premium,
    policy_premium_2
FROM enriched
ORDER BY approved_amount DESC, status DESC, id
LIMIT 20

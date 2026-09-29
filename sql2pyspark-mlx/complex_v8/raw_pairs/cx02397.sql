WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_TRUNC('DAY', reported_at) AS reported_at_day
    FROM claims
    WHERE claimed_amount <= 28900.9
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND MONTH(c.reported_at) IN (1, 3)
        )
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        a.team AS adjuster_team
    FROM qualified s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.policy_holder_id,
        s.policy_product,
        s.policy_end_date,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team_2,
        p.product AS policy_product_2
    FROM with_details_policy s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    approved_amount,
    reported_at,
    policy_holder_id,
    policy_product,
    policy_end_date,
    adjuster_level,
    adjuster_name,
    policy_product_2,
    COALESCE(adjuster_level, 0) AS adjuster_level_filled
FROM with_details_adjuster
ORDER BY policy_id DESC, id
LIMIT 20

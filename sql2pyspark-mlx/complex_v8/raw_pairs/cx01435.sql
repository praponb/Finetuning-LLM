WITH recent AS (
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
    WHERE claim_type IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id
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
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id DESC) AS rn
        FROM recent
    ) ranked
    WHERE rn = 1
),
combined_details AS (
    -- Attach adjuster attributes
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
        a.team AS adjuster_team
    FROM latest_per_key s
    JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Rank and compare rows within groups
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
        adjuster_team,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 2) AS policy_id_adjuster_id_avg_approved_amount,
        MAX(approved_amount) OVER (PARTITION BY policy_id, adjuster_id) AS policy_id_adjuster_id_max_approved_amount,
        COUNT(*) OVER w_policy_id_adjuster_id AS rows_in_policy_id_adjuster_id
    FROM combined_details
    WINDOW w_policy_id_adjuster_id AS (PARTITION BY policy_id, adjuster_id)
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.policy_id_adjuster_id_avg_approved_amount,
        p.start_date AS policy_start_date,
        po.region AS holder_region,
        po.risk_score AS holder_risk_score
    FROM windowed s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    policy_id_adjuster_id_avg_approved_amount,
    policy_start_date,
    holder_region,
    holder_risk_score,
    ABS(claimed_amount - 29359.4) AS claimed_amount_distance,
    DAYOFWEEK(claim_date) AS claim_date_dayofweek
FROM with_parent_policy
ORDER BY id

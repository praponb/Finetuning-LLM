WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 13
      AND claim_type = 'medical'
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id
    FROM recent s
    JOIN policies p ON p.id = s.policy_id AND p.premium > 1051.9
    WHERE p.holder_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        policy_premium,
        policy_holder_id,
        approved_amount - policy_premium AS approved_amount_minus_policy_premium,
        ABS(approved_amount - 17759.3) AS approved_amount_distance,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM combined_details_policy
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.policy_premium,
        s.policy_holder_id,
        s.approved_amount_distance,
        s.approved_amount_filled,
        p.premium AS policy_premium_2,
        p.start_date AS policy_start_date,
        p.status AS policy_status
    FROM scored s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.start_date >= DATE '2024-07-19'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.policy_premium,
        s.policy_holder_id,
        s.approved_amount_distance,
        s.approved_amount_filled,
        s.policy_premium_2,
        s.policy_status,
        p.risk_score AS holder_risk_score
    FROM joined s
    LEFT JOIN policyholders p ON p.id = s.policy_holder_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        policy_premium,
        policy_holder_id,
        approved_amount_distance,
        policy_premium_2,
        policy_status,
        holder_risk_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        ROW_NUMBER() OVER (ORDER BY approved_amount DESC, id) AS rn,
        SUM(policy_premium_2) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS policy_premium_2_sum_7,
        COUNT(*) OVER w_policy_holder_id_policy_stat AS rows_in_policy_holder_id_policy_status
    FROM with_details
    WINDOW w_policy_holder_id_policy_stat AS (PARTITION BY policy_holder_id, policy_status)
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.policy_premium,
        s.policy_holder_id,
        s.approved_amount_distance,
        s.policy_status,
        s.holder_risk_score,
        s.approved_amount_percent_rank,
        s.rn,
        s.policy_premium_2_sum_7,
        s.rows_in_policy_holder_id_policy_status,
        a.level AS adjuster_level,
        p.status AS policy_status_2,
        p.start_date AS policy_start_date
    FROM metrics s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    adjuster_id,
    approved_amount_distance,
    policy_status,
    holder_risk_score,
    approved_amount_percent_rank,
    rn,
    policy_premium_2_sum_7,
    rows_in_policy_holder_id_policy_status,
    policy_status_2,
    policy_start_date,
    LEAST(adjuster_level, approved_amount_distance) AS least_adjuster_level_approved_amount_distance
FROM with_parent_adjuster
ORDER BY rows_in_policy_holder_id_policy_status DESC, holder_risk_score, id
LIMIT 10

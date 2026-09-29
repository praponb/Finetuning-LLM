WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label,
        CASE WHEN claimed_amount >= 34763.4 THEN 'A' WHEN claimed_amount >= 10518.4 THEN 'B' ELSE 'C' END AS claimed_amount_tier
    FROM claims
    WHERE claim_type IS NOT NULL
      AND reported_at < TIMESTAMP '2025-03-08 14:00:00'
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.premium AS policy_premium,
        po.holder_name,
        po.region AS holder_region
    FROM valid s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        reported_at_label,
        claimed_amount_tier,
        holder_name,
        holder_region,
        policy_premium - approved_amount AS policy_premium_minus_approved_amount,
        CONCAT(holder_name, '#', CAST(adjuster_id AS STRING)) AS holder_name_ref,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM with_parent_policy
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, YEAR(claim_date) ORDER BY approved_amount ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        RANK() OVER (PARTITION BY policy_id, YEAR(claim_date) ORDER BY approved_amount ASC, id) AS approved_amount_rank
    FROM scored
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.reported_at_label,
        s.claimed_amount_tier,
        s.holder_name,
        s.policy_premium_minus_approved_amount,
        s.holder_name_ref,
        s.approved_amount_filled,
        s.approved_amount_avg_2_prior,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date AS claim_claim_date
    FROM windowed s
    JOIN claims c ON c.policy_id = s.policy_id
)
SELECT id, claim_id, field, metric_value
FROM (
    SELECT id, claim_id, approved_amount_filled, policy_premium_minus_approved_amount, claim_claimed_amount
    FROM exploded_detail_claims
) src
UNPIVOT (
    metric_value FOR field IN (approved_amount_filled, policy_premium_minus_approved_amount, claim_claimed_amount)
) unp

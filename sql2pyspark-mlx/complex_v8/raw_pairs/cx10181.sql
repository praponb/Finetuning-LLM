WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claimed_amount) OVER w_claim_type AS claim_type_max_claimed_amount,
        RANK() OVER w_claim_type_policy_id_by_clai AS claimed_amount_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type) AS distinct_policy_id_in_claim_type
    FROM recent
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_claim_type_policy_id_by_clai AS (PARTITION BY claim_type, policy_id ORDER BY claimed_amount ASC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        CASE WHEN claim_type IN ('fire', 'medical') THEN 'group_a' WHEN claim_type = 'flood' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM ranked
    WHERE adjuster_id <= 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_policy_id_in_claim_type, adjuster_id, claim_type, claim_type_max_claimed_amount
    FROM labeled
) src
PIVOT (
    MAX(claim_type_max_claimed_amount) AS max_v, SUM(claim_type_max_claimed_amount) AS sum_v
    FOR claim_type IN ('collision', 'flood', 'theft')
)

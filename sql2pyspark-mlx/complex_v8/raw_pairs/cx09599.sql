WITH base_adjusters AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE level < 3
      AND team = 'Property'
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.adjuster_name_len,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount
    FROM base_adjusters s
    JOIN claims c ON c.adjuster_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len_2,
        ROUND(claim_approved_amount * 0.25, 2) AS claim_approved_amount_adj
    FROM line_level_claims
),
with_parent_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.adjuster_name,
        s.adjuster_name_len,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_type,
        s.claim_policy_id,
        s.adjuster_name_len_2,
        s.claim_approved_amount_adj,
        c.approved_amount AS claim_approved_amount_2,
        c.reported_at AS claim_reported_at,
        p.premium AS policy_premium
    FROM scored s
    JOIN claims c ON c.id = s.claim_id
    JOIN policies p ON p.id = s.claim_policy_id
),
labeled AS (
    -- Derive helper columns
    SELECT *, ABS(adjuster_name_len_2 - 8) AS adjuster_name_len_2_distance
    FROM with_parent_claim
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_name_len_2 ORDER BY policy_premium, id, claim_id), 3) AS policy_premium_cume_dist,
        NTH_VALUE(adjuster_name_len, 2) OVER (PARTITION BY MONTH(claim_reported_at) ORDER BY adjuster_name_len_2, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_name_len,
        LAST_VALUE(claim_claimed_amount) IGNORE NULLS OVER (ORDER BY adjuster_name_len_2_distance DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_claimed_amount,
        ROW_NUMBER() OVER (ORDER BY adjuster_name_len_2_distance DESC, id, claim_id) AS adjuster_name_len_2_distance_seq
    FROM labeled
)
-- Final projection
SELECT
    id,
    adjuster_name,
    claim_id,
    claim_claimed_amount,
    claim_type,
    claim_policy_id,
    adjuster_name_len_2,
    claim_approved_amount_adj,
    claim_reported_at,
    policy_premium,
    policy_premium_cume_dist
FROM metrics
WHERE adjuster_name_len >= 6
ORDER BY claim_type DESC

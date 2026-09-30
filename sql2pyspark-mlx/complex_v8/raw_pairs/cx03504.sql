WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        UPPER(TRIM(status)) AS status_clean,
        ABS(approved_amount - 4554.8) AS approved_amount_distance,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 11
      AND approved_amount < 23207.7
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM recent s
    JOIN claims c ON c.policy_id = s.policy_id
),
adjuster_id_stats AS (
    -- Median claimed_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM with_children_claims
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM with_children_claims s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount >= g.median_claimed_amount
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        claim_id,
        claim_status,
        median_claimed_amount,
        claimed_amount_minus_median,
        HOUR(reported_at) AS reported_at_hour
    FROM above_average
    WHERE claim_type IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reported_at_hour, policy_id, claim_type, claimed_amount_minus_median
    FROM labeled
) src
PIVOT (
    AVG(claimed_amount_minus_median)
    FOR claim_type IN ('delay', 'collision')
)

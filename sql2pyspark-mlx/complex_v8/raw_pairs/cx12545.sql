WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount
    FROM claims
    WHERE status = 'PAID'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM scoped_claims
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM labeled s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'F%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_min_adjuster_level,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_team ORDER BY adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SUM(adjuster_level) OVER (PARTITION BY adjuster_team ORDER BY adjuster_level, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS adjuster_level_sum_2_prior,
        LAG(approved_amount, 2) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount, id) AS prev2_approved_amount
    FROM with_details
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY claim_type ORDER BY adjuster_level_sum_2_prior, id) AS adjuster_level_sum_2_prior_rank,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY second_approved_amount) AS second_approved_amount_rank
    FROM with_windows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_team, adjuster_id, claim_type, adjuster_level_sum_2_prior
    FROM with_windows_2
) src
PIVOT (
    AVG(adjuster_level_sum_2_prior)
    FOR claim_type IN ('flood', 'fire')
)

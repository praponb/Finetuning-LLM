WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        ABS(claimed_amount - 20710.1) AS claimed_amount_distance,
        ABS(claimed_amount - 28755.2) AS claimed_amount_distance_2
    FROM claims
    WHERE claim_type <> 'delay'
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.claimed_amount_distance_2,
        a.team AS adjuster_team
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Motor', 'Travel')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        NTILE(5) OVER (PARTITION BY adjuster_team, claim_type ORDER BY approved_amount NULLS LAST, id) AS approved_amount_quintile,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        FIRST_VALUE(claimed_amount_distance_2) IGNORE NULLS OVER (PARTITION BY claim_type, adjuster_team ORDER BY claimed_amount_distance_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount_distance_2,
        ROUND(claimed_amount_distance_2 - AVG(claimed_amount_distance_2) OVER (PARTITION BY claim_type), 2) AS claimed_amount_distance_2_vs_claim_type_avg,
        ROW_NUMBER() OVER w_all_by_reported_at AS seq_no,
        COUNT(*) OVER (PARTITION BY claim_type, adjuster_team ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(claimed_amount_distance_2 - AVG(claimed_amount_distance_2) OVER w_policy_id, 2) AS claimed_amount_distance_2_vs_policy_id_avg,
        COUNT(*) OVER w_claim_type AS rows_in_claim_type,
        MIN(claimed_amount_distance_2) OVER w_policy_id AS policy_id_min_claimed_amount_distance_2
    FROM with_parent_adjuster
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at, id), w_policy_id AS (PARTITION BY policy_id), w_claim_type AS (PARTITION BY claim_type)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        approved_amount_quintile,
        MIN(approved_amount) AS min_approved_amount,
        MAX(reported_at) AS last_reported_at
    FROM metrics
    GROUP BY 1
    HAVING SUM(running_count) > 4
)
-- Final projection
SELECT approved_amount_quintile, min_approved_amount, last_reported_at
FROM rollup_base
WHERE min_approved_amount < 35000.0

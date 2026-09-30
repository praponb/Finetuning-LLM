WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        CASE WHEN claim_type IN ('medical', 'theft') THEN 'group_a' WHEN claim_type = 'flood' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE approved_amount >= 0.0
      AND claim_type = 'flood'
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        s.claim_type_group,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM valid_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        claim_type_group,
        adjuster_team,
        policy_premium,
        policy_status,
        SUM(policy_premium) OVER (PARTITION BY policy_status) AS policy_status_sum_policy_premium,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, claim_type_group ORDER BY reported_at DESC), 3) AS reported_at_cume_dist,
        MAX(policy_premium) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_policy_premium,
        FIRST_VALUE(claim_date) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        DENSE_RANK() OVER (ORDER BY reported_at) AS reported_at_dense_rank,
        FIRST_VALUE(claim_date) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_2,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date
    FROM with_details_adjuster
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        claim_type_group,
        adjuster_team,
        policy_premium,
        policy_status,
        policy_status_sum_policy_premium,
        distinct_policy_id_in_adjuster_id,
        reported_at_cume_dist,
        adjuster_id_max_policy_premium,
        first_claim_date,
        first_claim_date_2,
        second_claim_date
    FROM sequenced
    WHERE reported_at_dense_rank <= 5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        claim_type_group,
        adjuster_team,
        policy_premium,
        policy_status,
        distinct_policy_id_in_adjuster_id,
        reported_at_cume_dist,
        adjuster_id_max_policy_premium,
        first_claim_date_2,
        second_claim_date,
        COUNT(*) OVER w_distinct_policy_id_in_adjust AS rows_in_distinct_policy_id_in_adjuster_id_first_,
        LEAD(first_claim_date, 2) OVER w_claim_type_by_distinct_polic AS next2_first_claim_date,
        COUNT(*) OVER (ORDER BY first_claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (ORDER BY first_claim_date_2, id) AS first_claim_date_2_rank
    FROM top_rows
    WINDOW w_distinct_policy_id_in_adjust AS (PARTITION BY distinct_policy_id_in_adjuster_id, DATE_TRUNC('MONTH', first_claim_date)), w_claim_type_by_distinct_polic AS (PARTITION BY claim_type ORDER BY distinct_policy_id_in_adjuster_id DESC, id)
)
SELECT
    id,
    claim_date,
    claim_type,
    reported_at,
    claim_type_group,
    adjuster_team,
    policy_status,
    reported_at_cume_dist,
    adjuster_id_max_policy_premium,
    first_claim_date_2,
    second_claim_date,
    rows_in_distinct_policy_id_in_adjuster_id_first_,
    next2_first_claim_date,
    running_count,
    first_claim_date_2_rank,
    CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
    CASE WHEN reported_at_cume_dist >= 0.8 THEN 'large' WHEN reported_at_cume_dist >= 0.2 THEN 'medium' ELSE 'small' END AS reported_at_cume_dist_tier
FROM metrics
ORDER BY adjuster_id_max_policy_premium NULLS LAST, rows_in_distinct_policy_id_in_adjuster_id_first_ DESC, id
LIMIT 20

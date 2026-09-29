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
    WHERE TO_DATE(reported_at) < DATE '2025-02-12'
      AND claim_type = 'fire'
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level <= 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status)) AS distinct_claim_type_in_status,
        COUNT(*) OVER (PARTITION BY adjuster_level) AS rows_in_adjuster_level,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type) AS distinct_policy_id_in_claim_type,
        MAX(adjuster_level) OVER (PARTITION BY policy_id, adjuster_level ORDER BY adjuster_level NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_adjuster_level_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, YEAR(reported_at) ORDER BY claimed_amount), 3) AS claimed_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        SUM(adjuster_level) OVER w_claim_type AS claim_type_sum_adjuster_level,
        LEAD(claimed_amount) OVER w_claim_type_policy_id_by_repo AS next_claimed_amount
    FROM combined_details_adjuster
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_claim_type_policy_id_by_repo AS (PARTITION BY claim_type, policy_id ORDER BY reported_at DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(reported_at_percent_rank, 2) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_percent_rank,
        MAX(reported_at_percent_rank) OVER (PARTITION BY policy_id) AS policy_id_max_reported_at_percent_rank,
        FIRST_VALUE(next_claimed_amount) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_claimed_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        SUM(reported_at_percent_rank) OVER (PARTITION BY policy_id) AS policy_id_sum_reported_at_percent_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    adjuster_name,
    adjuster_level,
    distinct_claim_type_in_status,
    rows_in_adjuster_level,
    distinct_policy_id_in_claim_type,
    max_adjuster_level_so_far,
    reported_at_percent_rank,
    claim_type_sum_adjuster_level,
    next_claimed_amount,
    second_reported_at_percent_rank,
    distinct_adjuster_id_in_policy_id,
    claimed_amount + next_claimed_amount AS claimed_amount_plus_next_claimed_amount,
    ROUND(approved_amount / NULLIF(reported_at_percent_rank, 0), 3) AS approved_amount_per_reported_at_percent_rank
FROM with_windows

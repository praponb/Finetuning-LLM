WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE approved_amount >= 4234.9
      AND claim_date < DATE '2025-05-26'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type), 0), 4) AS approved_amount_share_of_claim_type,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY reported_at), 3) AS reported_at_cume_dist,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at), 3) AS reported_at_percent_rank,
        LEAD(reported_at) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at, id) AS next_reported_at,
        COUNT(*) OVER (PARTITION BY policy_id, status) AS rows_in_policy_id_status,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at) AS reported_at_dense_rank
    FROM valid
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount_share_of_claim_type,
        second_claimed_amount,
        next_reported_at,
        SUM(reported_at_dense_rank) OVER (PARTITION BY claim_type, reported_at_dense_rank ORDER BY reported_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_reported_at_dense_rank,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id, policy_id)) AS distinct_claim_type_in_adjuster_id_policy_id,
        ROUND(rows_in_policy_id_status - AVG(rows_in_policy_id_status) OVER (PARTITION BY adjuster_id, policy_id), 2) AS rows_in_policy_id_status_vs_adjuster_id_policy_i,
        RANK() OVER (PARTITION BY claim_type, reported_at_dense_rank ORDER BY reported_at_cume_dist DESC, id) AS reported_at_cume_dist_rank,
        MIN(reported_at_cume_dist) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_min_reported_at_cume_dist,
        ROUND(CUME_DIST() OVER w_claim_type_reported_at_dense_3, 3) AS reported_at_cume_dist_cume_dist,
        ROUND(second_claimed_amount - AVG(second_claimed_amount) OVER w_adjuster_id_policy_id, 2) AS second_claimed_amount_vs_adjuster_id_policy_id_a
    FROM with_windows
    WINDOW w_claim_type_reported_at_dense_3 AS (PARTITION BY claim_type, reported_at_dense_rank ORDER BY reported_at_cume_dist DESC), w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id)
)
-- Final projection
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount_share_of_claim_type,
    second_claimed_amount,
    next_reported_at,
    running_reported_at_dense_rank,
    distinct_claim_type_in_adjuster_id_policy_id,
    reported_at_cume_dist_rank,
    adjuster_id_policy_id_min_reported_at_cume_dist,
    reported_at_cume_dist_cume_dist,
    second_claimed_amount_vs_adjuster_id_policy_id_a,
    ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
    FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
FROM sequenced
ORDER BY adjuster_id_policy_id_min_reported_at_cume_dist DESC NULLS FIRST, approved_amount_share_of_claim_type NULLS LAST

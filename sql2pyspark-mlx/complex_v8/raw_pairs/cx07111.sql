WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_date_label,
        s.claimed_amount_adj,
        p.premium AS policy_premium,
        p.status AS policy_status,
        a.level AS adjuster_level
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_type,
        reported_at,
        claimed_amount_adj,
        policy_premium,
        policy_status,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, claim_type), 0), 4) AS approved_amount_share_of_policy_id_claim_type,
        RANK() OVER (PARTITION BY adjuster_level ORDER BY policy_premium DESC NULLS FIRST) AS policy_premium_rank,
        LAG(approved_amount) OVER (PARTITION BY claim_type, policy_status ORDER BY policy_premium, id) AS prev_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY YEAR(reported_at) ORDER BY claimed_amount_adj, id) AS rn,
        MAX(adjuster_level) OVER w_policy_id_claim_type AS policy_id_claim_type_max_adjuster_level,
        SUM(policy_premium) OVER (PARTITION BY YEAR(reported_at) ORDER BY claimed_amount_adj, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS policy_premium_sum_3_prior,
        ROUND(AVG(adjuster_level) OVER w_policy_id_claim_type, 2) AS policy_id_claim_type_avg_adjuster_level,
        ROUND(claimed_amount_adj / NULLIF(SUM(claimed_amount_adj) OVER (PARTITION BY policy_id, claim_type), 0), 4) AS claimed_amount_adj_share_of_policy_id_claim_type,
        MAX(policy_premium) OVER (PARTITION BY adjuster_level ORDER BY policy_premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_policy_premium_so_far,
        COUNT(*) OVER (PARTITION BY policy_id, claim_type) AS rows_in_policy_id_claim_type
    FROM with_details_policy
    WINDOW w_policy_id_claim_type AS (PARTITION BY policy_id, claim_type)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rows_in_policy_id_claim_type) OVER (PARTITION BY policy_premium_rank, rn), 2) AS policy_premium_rank_rn_avg_rows_in_policy_id_cla,
        DENSE_RANK() OVER (PARTITION BY policy_status ORDER BY policy_id_claim_type_avg_adjuster_level DESC, id) AS policy_id_claim_type_avg_adjuster_level_dense_ra,
        SUM(policy_premium_sum_3_prior) OVER (ORDER BY policy_id_claim_type_max_adjuster_level, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_policy_premium_sum_3_prior,
        NTILE(10) OVER (PARTITION BY policy_status ORDER BY policy_id_claim_type_avg_adjuster_level DESC, id) AS policy_id_claim_type_avg_adjuster_level_decile,
        NTH_VALUE(policy_premium_sum_3_prior, 2) OVER (PARTITION BY rn, policy_status ORDER BY policy_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_premium_sum_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rn, policy_status ORDER BY policy_premium), 3) AS policy_premium_percent_rank,
        ROUND(AVG(prev_approved_amount) OVER (PARTITION BY policy_status, rn), 2) AS policy_status_rn_avg_prev_approved_amount,
        SIZE(COLLECT_SET(claim_type) OVER w_policy_status_rn) AS distinct_claim_type_in_policy_status_rn
    FROM metrics
    WINDOW w_policy_status_rn AS (PARTITION BY policy_status, rn)
)
-- Final projection
SELECT
    id,
    claim_type,
    reported_at,
    claimed_amount_adj,
    policy_premium,
    policy_status,
    approved_amount_share_of_policy_id_claim_type,
    policy_premium_rank,
    prev_approved_amount,
    rn,
    policy_id_claim_type_max_adjuster_level,
    policy_premium_sum_3_prior,
    policy_id_claim_type_avg_adjuster_level,
    claimed_amount_adj_share_of_policy_id_claim_type,
    max_policy_premium_so_far,
    rows_in_policy_id_claim_type,
    policy_premium_rank_rn_avg_rows_in_policy_id_cla,
    policy_id_claim_type_avg_adjuster_level_dense_ra,
    policy_id_claim_type_avg_adjuster_level_decile,
    policy_premium_percent_rank,
    policy_status_rn_avg_prev_approved_amount,
    distinct_claim_type_in_policy_status_rn,
    GREATEST(prev_approved_amount, policy_premium_rank) AS greatest_prev_approved_amount_policy_premium_ran,
    CASE WHEN claimed_amount_adj >= 41295.0 THEN 'HIGH' WHEN claimed_amount_adj >= 26315.0 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_adj_tier
FROM with_windows

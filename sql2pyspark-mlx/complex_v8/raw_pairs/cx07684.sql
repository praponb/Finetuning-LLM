WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        CASE WHEN approved_amount >= 14377.6 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE status = 'REJECTED'
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.approved_amount_tier,
        s.approved_amount_plus_claimed_amount,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM valid_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(policy_premium) OVER w_adjuster_id AS adjuster_id_min_policy_premium,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY reported_at, id)) AS days_since_prev_claim_date,
        RANK() OVER (ORDER BY claim_date DESC, id) AS claim_date_rank,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_adjuster_id, 2) AS adjuster_level_vs_adjuster_id_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount DESC, id), 3) AS claimed_amount_percent_rank,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        MAX(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_max_claimed_amount,
        LAG(adjuster_level) OVER w_all_by_claim_date AS prev_adjuster_level,
        ROW_NUMBER() OVER w_all_by_claim_date AS seq_no,
        LEAD(reported_at) OVER w_all_by_claimed_amount AS next_reported_at,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY policy_premium DESC, id)) AS days_since_prev_claim_date_2,
        adjuster_level - LAG(adjuster_level) OVER (ORDER BY policy_premium DESC, id) AS adjuster_level_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY adjuster_level NULLS LAST), 3) AS adjuster_level_percent_rank
    FROM with_details_adjuster
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_all_by_claim_date AS (ORDER BY claim_date DESC, id), w_all_by_claimed_amount AS (ORDER BY claimed_amount DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount_tier,
        approved_amount_plus_claimed_amount,
        adjuster_team,
        policy_product,
        policy_premium,
        adjuster_id_min_policy_premium,
        days_since_prev_claim_date,
        claim_date_rank,
        adjuster_level_vs_adjuster_id_avg,
        claimed_amount_percent_rank,
        policy_id_max_claimed_amount,
        prev_adjuster_level,
        seq_no,
        next_reported_at,
        adjuster_level_change,
        NTILE(4) OVER w_all_by_policy_premium AS policy_premium_quartile,
        COUNT(*) OVER (ORDER BY adjuster_id_min_policy_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(next_reported_at, 2) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_reported_at,
        SUM(prev_adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_sum_prev_adjuster_level,
        LAG(prev_adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), approved_amount_tier ORDER BY reported_at DESC, id) AS prev_prev_adjuster_level,
        RANK() OVER w_all_by_adjuster_level_ties_2 AS adjuster_level_rank,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), approved_amount_tier ORDER BY reported_at DESC) AS reported_at_dense_rank,
        ROUND(AVG(adjuster_level_change) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_change_avg_6_prior
    FROM windowed
    WINDOW w_all_by_policy_premium AS (ORDER BY policy_premium DESC, id), w_all_by_adjuster_level_ties_2 AS (ORDER BY adjuster_level DESC)
)
SELECT
    id,
    adjuster_id,
    claim_date,
    approved_amount_plus_claimed_amount,
    adjuster_team,
    policy_product,
    policy_premium,
    days_since_prev_claim_date,
    claim_date_rank,
    adjuster_level_vs_adjuster_id_avg,
    claimed_amount_percent_rank,
    prev_adjuster_level,
    seq_no,
    next_reported_at,
    policy_premium_quartile,
    second_next_reported_at,
    prev_prev_adjuster_level,
    adjuster_level_rank,
    reported_at_dense_rank,
    adjuster_level_change_avg_6_prior
FROM with_windows
ORDER BY second_next_reported_at

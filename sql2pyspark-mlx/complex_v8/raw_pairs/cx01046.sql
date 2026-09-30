WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE (status = 'APPROVED' OR claim_type IN ('theft', 'delay', 'flood'))
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount_filled DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        MIN(claimed_amount_filled) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_filled_so_far,
        RANK() OVER w_claim_date_year_by_reported AS reported_at_rank,
        ROW_NUMBER() OVER w_all_by_claimed_amount AS rn,
        MIN(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_claimed_amount,
        LEAD(claimed_amount_filled) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at DESC, id) AS next_claimed_amount_filled,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount_filled DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_max_3,
        LEAD(reported_at) OVER w_claim_date_year_by_reported AS next_reported_at,
        FIRST_VALUE(reported_at) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        ROUND(claimed_amount_filled - AVG(claimed_amount_filled) OVER (PARTITION BY adjuster_id), 2) AS claimed_amount_filled_vs_adjuster_id_avg,
        SUM(claimed_amount_filled) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_filled,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount), 3) AS claimed_amount_cume_dist,
        LEAD(claim_date) OVER (ORDER BY claimed_amount, id) AS next_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS claimed_amount_share_of_adjuster_id
    FROM scoped
    WINDOW w_claim_date_year_by_reported AS (PARTITION BY YEAR(claim_date) ORDER BY reported_at DESC, id), w_all_by_claimed_amount AS (ORDER BY claimed_amount, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    status,
    reported_at,
    claimed_amount_filled,
    claimed_amount_avg_2_prior,
    min_claimed_amount_filled_so_far,
    reported_at_rank,
    rn,
    adjuster_id_min_claimed_amount,
    next_claimed_amount_filled,
    next_reported_at,
    first_reported_at,
    claimed_amount_filled_vs_adjuster_id_avg,
    running_claimed_amount_filled,
    claimed_amount_cume_dist,
    next_claim_date,
    ABS(claimed_amount_cume_dist - 0.7) AS claimed_amount_cume_dist_distance
FROM with_windows
ORDER BY status

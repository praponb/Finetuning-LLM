WITH scoped_claims AS (
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
        reported_at,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM claims
    WHERE adjuster_id <= 6
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(claim_date) ORDER BY claimed_amount DESC, id) AS claimed_amount_rank,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY status, MONTH(reported_at) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY status ORDER BY reported_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_1_prior,
        ROUND(PERCENT_RANK() OVER w_status_by_reported_at_ties, 3) AS reported_at_percent_rank,
        MIN(approved_amount_adj) OVER (PARTITION BY status) AS status_min_approved_amount_adj,
        SUM(approved_amount_adj) OVER w_status AS status_sum_approved_amount_adj,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status), 0), 4) AS claimed_amount_share_of_status,
        DENSE_RANK() OVER (PARTITION BY status, MONTH(reported_at) ORDER BY claimed_amount DESC) AS claimed_amount_dense_rank
    FROM scoped_claims
    WINDOW w_status_by_reported_at_ties AS (PARTITION BY status ORDER BY reported_at ASC), w_status AS (PARTITION BY status)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE claimed_amount_rank = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    approved_amount_adj,
    claimed_amount_rank,
    claimed_amount_sum_1_prior,
    reported_at_percent_rank,
    status_min_approved_amount_adj,
    status_sum_approved_amount_adj,
    claimed_amount_vs_status_avg,
    claimed_amount_share_of_status,
    claimed_amount_dense_rank
FROM leaders
ORDER BY claimed_amount_dense_rank DESC, policy_id DESC, id
LIMIT 20

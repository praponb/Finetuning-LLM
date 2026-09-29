WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE claimed_amount >= 10673.3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        approved_amount_plus_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER w_claim_type_reported_at_month AS claimed_amount_change,
        LAG(claimed_amount, 1, 0) OVER w_claim_type_reported_at_month AS prev_claimed_amount,
        ROUND(CUME_DIST() OVER w_claim_type_reported_at_month, 3) AS claimed_amount_cume_dist
    FROM prepared_claims
    WINDOW w_claim_type_reported_at_month AS (PARTITION BY claim_type, MONTH(reported_at) ORDER BY claimed_amount DESC, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_type,
        reported_at,
        approved_amount_plus_claimed_amount,
        claim_type_avg_claimed_amount,
        claimed_amount_change,
        prev_claimed_amount,
        RANK() OVER (ORDER BY prev_claimed_amount DESC, id) AS prev_claimed_amount_rank,
        RANK() OVER (ORDER BY prev_claimed_amount DESC) AS prev_claimed_amount_rank_2,
        MAX(prev_claimed_amount) OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY reported_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_claimed_amount_max_6_prior
    FROM with_trends
)
-- Subtotals with CUBE
SELECT
    prev_claimed_amount_rank_2,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING(prev_claimed_amount_rank_2) AS is_total_prev_claimed_amount_rank_2,
    ROUND(STDDEV(prev_claimed_amount), 2) AS stddev_prev_claimed_amount
FROM sequenced
GROUP BY CUBE(prev_claimed_amount_rank_2, claim_type)
ORDER BY prev_claimed_amount_rank_2 NULLS LAST, claim_type NULLS LAST

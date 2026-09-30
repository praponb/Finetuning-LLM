WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount,
        c.avg_claim_approved_amount,
        c.last_claim_reported_at
    FROM in_window s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(claim_date), product ORDER BY avg_claim_approved_amount DESC) AS avg_claim_approved_amount_dense_rank,
        MAX(avg_claim_approved_amount) OVER (PARTITION BY product, status) AS product_status_max_avg_claim_approved_amount,
        NTH_VALUE(min_claim_claimed_amount, 2) OVER (PARTITION BY status ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_claim_claimed_amount,
        MAX(min_claim_claimed_amount) OVER (PARTITION BY MONTH(claim_date), product ORDER BY avg_claim_approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS min_claim_claimed_amount_max_2_prior,
        LAST_VALUE(last_claim_reported_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(last_claim_reported_at) ORDER BY avg_claim_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_claim_reported_at,
        LAG(claim_date, 3) OVER (PARTITION BY TO_DATE(last_claim_reported_at) ORDER BY avg_claim_approved_amount, id) AS prev3_claim_date
    FROM profiled
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.adjuster_id,
        s.status,
        s.premium,
        s.min_claim_claimed_amount,
        s.avg_claim_approved_amount,
        s.product_status_max_avg_claim_approved_amount,
        s.second_min_claim_claimed_amount,
        s.min_claim_claimed_amount_max_2_prior,
        s.last_known_last_claim_reported_at,
        s.prev3_claim_date,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM sequenced s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT DISTINCT min_claim_claimed_amount
FROM combined_details_adjuster
ORDER BY min_claim_claimed_amount

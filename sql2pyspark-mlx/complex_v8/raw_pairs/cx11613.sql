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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        claimed_amount,
        product,
        premium,
        LAG(claimed_amount) OVER w_all_by_claim_date AS prev_claimed_amount,
        MAX(claimed_amount) OVER w_adjuster_id AS adjuster_id_max_claimed_amount
    FROM in_window
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date, id), w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    claimed_amount,
    product,
    adjuster_id_max_claimed_amount,
    FLOOR(prev_claimed_amount / 10000) * 10000 AS prev_claimed_amount_bucket,
    COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
FROM sequenced
ORDER BY id

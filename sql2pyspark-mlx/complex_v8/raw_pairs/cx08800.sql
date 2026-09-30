WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        MIN(claimed_amount) OVER w_adjuster_id AS adjuster_id_min_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id) AS seq_no
    FROM in_window
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    adjuster_id,
    approved_amount,
    product,
    adjuster_id_min_claimed_amount,
    adjuster_id_max_approved_amount
FROM with_trends
ORDER BY id

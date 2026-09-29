WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claimed_amount,
        claim_type,
        approved_amount,
        premium,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY product ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type, policy_id), 0), 4) AS approved_amount_share_of_claim_type_policy_id,
        DENSE_RANK() OVER (ORDER BY claimed_amount, id) AS claimed_amount_dense_rank,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY claimed_amount ASC, id) AS claimed_amount_dense_rank_2,
        ROUND(PERCENT_RANK() OVER w_all_by_claimed_amount_ties, 3) AS claimed_amount_percent_rank
    FROM in_window
    WINDOW w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount)
)
SELECT
    id,
    policy_id,
    claimed_amount,
    claim_type,
    first_known_claimed_amount,
    approved_amount_share_of_claim_type_policy_id,
    claimed_amount_dense_rank,
    claimed_amount_dense_rank_2,
    ABS(approved_amount - 15477.7) AS approved_amount_distance
FROM metrics
ORDER BY claimed_amount_dense_rank_2 DESC, approved_amount_share_of_claim_type_policy_id DESC

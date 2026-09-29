WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM joined_ranges
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM joined_ranges)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date
    FROM with_activity s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY claim_type, policy_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claim_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_max_3,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY claim_date DESC) AS claim_date_dense_rank,
        NTILE(4) OVER w_claim_type_policy_id_by_prem_2 AS premium_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, policy_id ORDER BY premium ASC), 3) AS premium_percent_rank
    FROM profiled
    WINDOW w_claim_type_policy_id_by_prem_2 AS (PARTITION BY claim_type, policy_id ORDER BY premium ASC, id)
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    product,
    premium,
    second_claimed_amount
FROM sequenced
ORDER BY claim_date, id DESC

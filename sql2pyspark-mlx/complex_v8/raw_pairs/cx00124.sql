WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY policy_id) AS policy_id_max_premium,
        SUM(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER w_policy_id) AS distinct_claim_type_in_policy_id,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        ROUND(premium / NULLIF(SUM(premium) OVER w_policy_id, 0), 4) AS premium_share_of_policy_id
    FROM in_window
    WINDOW w_policy_id AS (PARTITION BY policy_id)
)
SELECT
    id,
    policy_id,
    claimed_amount,
    claim_type,
    adjuster_id,
    product,
    premium,
    policy_id_max_premium,
    policy_id_sum_claimed_amount,
    distinct_claim_type_in_policy_id,
    rows_in_policy_id,
    premium_share_of_policy_id
FROM sequenced
WHERE distinct_claim_type_in_policy_id >= 3
ORDER BY claim_type, adjuster_id, id
LIMIT 20

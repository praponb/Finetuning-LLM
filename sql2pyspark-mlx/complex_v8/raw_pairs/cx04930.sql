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
latest_per_key AS (
    -- Keep the latest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        adjuster_id,
        status,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id DESC) AS recency_rank
        FROM in_window
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    status,
    product
FROM latest_per_key
WHERE status NOT IN ('IN_REVIEW', 'REJECTED')
ORDER BY id DESC
LIMIT 20

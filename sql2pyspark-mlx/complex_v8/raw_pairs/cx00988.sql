WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
deduped AS (
    -- Keep the earliest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date, id) AS row_num
        FROM in_window
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    product,
    premium
FROM deduped
WHERE approved_amount <= 15857.9
ORDER BY policy_id, product, id
LIMIT 20

WITH joined_ranges AS (
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
unique_rows AS (
    -- Keep the latest row per adjuster_id, product
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
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, product ORDER BY claim_date DESC, id DESC) AS rn
        FROM joined_ranges
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    premium
FROM unique_rows
ORDER BY id NULLS LAST

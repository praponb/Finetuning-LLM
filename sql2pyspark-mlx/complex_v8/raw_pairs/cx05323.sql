WITH covered AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
unique_rows AS (
    -- Keep the latest row per policy_id, claim_type
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        approved_amount,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_date DESC, id DESC) AS rn
        FROM covered
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM unique_rows
    WHERE claimed_amount > (SELECT MIN(claimed_amount) FROM unique_rows)
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    claim_type,
    product,
    premium,
    FLOOR(premium / 1000) * 1000 AS premium_bucket
FROM without_matches
ORDER BY claimed_amount DESC, claim_date DESC NULLS FIRST, id
LIMIT 20

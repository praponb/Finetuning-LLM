WITH covered AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product = 'TRAVEL'
              AND p.premium > 2289.0
        )
),
filtered_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM filtered)
)
SELECT
    id,
    claim_date,
    claimed_amount,
    product,
    premium,
    LENGTH(TRIM(claim_type)) AS claim_type_len,
    claimed_amount - premium AS claimed_amount_minus_premium
FROM filtered_2

WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.status,
        s.approved_amount,
        p.premium AS policy_premium
    FROM joined_ranges s
    JOIN policies p ON p.id = s.policy_id
)
SELECT id, measure, reading
FROM (
    SELECT id, approved_amount, policy_premium
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (approved_amount, policy_premium)
) unp

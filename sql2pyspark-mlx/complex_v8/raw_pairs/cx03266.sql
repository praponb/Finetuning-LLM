WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        status,
        product,
        ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m
    FROM joined_ranges
    WHERE premium < 3465.4
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    status,
    product,
    claim_date_plus_6m,
    DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
    ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj
FROM derived
WHERE product = 'LIFE'

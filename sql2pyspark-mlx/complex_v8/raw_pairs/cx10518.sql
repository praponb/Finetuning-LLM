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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        approved_amount,
        claim_type,
        product,
        premium,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        ROUND(claimed_amount / NULLIF(premium, 0), 3) AS claimed_amount_per_premium
    FROM joined_ranges
    WHERE approved_amount <= 32878.5
)
SELECT id, field, reading
FROM (
    SELECT id, premium, claimed_amount_per_premium
    FROM annotated
) src
UNPIVOT (
    reading FOR field IN (premium AS m_premium, claimed_amount_per_premium AS m_claimed_amount_per_premium)
) unp

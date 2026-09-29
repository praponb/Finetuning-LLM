WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        ABS(premium - 3521.8) AS premium_distance,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE product IS NOT NULL
      AND holder_id <= 10
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_adj,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount
    FROM cleaned_policys s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claimed_amount <= 33690.0
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium_adj,
        s.claim_id,
        s.claim_type,
        s.claim_claimed_amount,
        p.risk_score AS holder_risk_score
    FROM detail_rows_claims s
    JOIN policyholders p ON p.id = s.holder_id
),
per_group AS (
    -- Summarise per group
    SELECT
        claim_type,
        claim_id,
        holder_id,
        COUNT(DISTINCT claim_id) AS distinct_claim_ids,
        MIN(claim_claimed_amount) AS min_claim_claimed_amount
    FROM joined
    WHERE claim_id >= 25
    GROUP BY claim_type, claim_id, holder_id
)
SELECT *
FROM (
    SELECT claim_id, distinct_claim_ids, claim_type, min_claim_claimed_amount
    FROM per_group
) src
PIVOT (
    MIN(min_claim_claimed_amount)
    FOR claim_type IN ('flood', 'theft', 'collision')
)

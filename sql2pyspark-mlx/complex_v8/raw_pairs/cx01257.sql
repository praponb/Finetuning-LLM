WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        status,
        product,
        ROUND(100 * COUNT(CASE WHEN claim_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_claim_type_is_not_null,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(premium), 0), 3) AS claimed_amount_to_premium_ratio
    FROM matched
    GROUP BY 1, 2, 3
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE pct_claim_type_is_not_null > (SELECT AVG(pct_claim_type_is_not_null) FROM stats)
),
labeled AS (
    -- Derive helper columns
    SELECT
        claim_type,
        status,
        product,
        pct_claim_type_is_not_null,
        claimed_amount_to_premium_ratio,
        LENGTH(TRIM(status)) AS status_len,
        LENGTH(TRIM(status)) AS status_len_2
    FROM with_activity
    WHERE status NOT IN ('PAID', 'IN_REVIEW')
)
SELECT
    claim_type,
    status,
    product,
    pct_claim_type_is_not_null,
    claimed_amount_to_premium_ratio,
    status_len,
    status_len_2
FROM labeled
ORDER BY claim_type DESC, status

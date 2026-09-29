WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.adjuster_id,
        s.approved_amount,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ADD_MONTHS(claim_claim_date, 1) AS claim_claim_date_plus_1m,
        DATE_FORMAT(claim_claim_date, 'MMM yyyy') AS claim_claim_date_label,
        DATE_TRUNC('MONTH', claim_claim_date) AS claim_claim_date_month
    FROM with_children_claims
    WHERE approved_amount > 9389.3
)
SELECT
    id,
    approved_amount,
    product,
    claim_id,
    claim_adjuster_id,
    claim_claimed_amount,
    claim_claim_date_month,
    SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
    DATE_FORMAT(claim_claim_date_month, 'yyyy-MM') AS claim_claim_date_month_label
FROM enriched
ORDER BY claim_claim_date_month DESC

WITH covered AS (
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
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', status, product) AS status_product_label,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM covered
    WHERE claim_date BETWEEN DATE '2024-04-09' AND DATE '2025-08-25'
)
SELECT
    id,
    policy_id,
    claim_date,
    status,
    approved_amount,
    product,
    premium,
    status_product_label,
    status_prefix,
    DATE_TRUNC('MONTH', claim_date) AS claim_date_month
FROM labeled
ORDER BY premium DESC NULLS FIRST, policy_id DESC, id
LIMIT 5

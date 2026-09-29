WITH joined_ranges AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        status,
        product,
        premium,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        CASE WHEN claim_type IN ('fire', 'flood') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM joined_ranges
)
SELECT
    id,
    claim_date,
    product,
    premium,
    claim_type_group,
    CONCAT_WS('|', product, claim_type_group) AS product_claim_type_group_label,
    DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date_2
FROM tagged
ORDER BY claim_type_group, claim_date DESC

WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(premium * 0.25, 2) AS premium_adj,
        CONCAT_WS('-', claim_type, product) AS claim_type_product_label,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj
    FROM in_window
    WHERE adjuster_id <= 6
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.team AS adjuster_team, a.adjuster_name
    FROM annotated s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    product,
    premium_adj,
    claim_type_product_label,
    claimed_amount_adj,
    adjuster_team,
    CASE WHEN claimed_amount_adj >= 60020.0 THEN 'A' WHEN claimed_amount_adj >= 22570.0 THEN 'B' ELSE 'C' END AS claimed_amount_adj_tier,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM combined_details_adjuster
ORDER BY premium_adj DESC, id
LIMIT 10

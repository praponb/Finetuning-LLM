WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.approved_amount) AS prior_approved_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_60d,
        s.prior_approved_amount_60d,
        p.product AS policy_product
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product = 'TRAVEL'
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_approved_amount_60d,
    policy_product,
    CASE WHEN policy_product IN ('HOME', 'TRAVEL') THEN 'group_a' WHEN policy_product = 'AUTO' THEN 'group_b' ELSE 'other' END AS policy_product_group
FROM combined_details_policy
WHERE policy_id <= 3

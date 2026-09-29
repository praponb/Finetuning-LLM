WITH joined_ranges AS (
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
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_60d,
        s.prior_approved_amount_60d,
        p.product AS policy_product
    FROM joined_ranges s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    policy_product,
    COALESCE(prior_rows_60d, 0) AS prior_rows_60d_filled,
    CONCAT(policy_product, '#', CAST(id AS STRING)) AS policy_product_ref
FROM with_parent
ORDER BY claim_date DESC, policy_product DESC NULLS FIRST

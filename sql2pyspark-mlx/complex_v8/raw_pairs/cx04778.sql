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
scored AS (
    -- Add calculated columns
    SELECT *, DATE_ADD(claim_date, 90) AS claim_date_plus_90d
    FROM in_window
)
-- Final projection
SELECT
    id,
    prior_rows_60d,
    claim_date_plus_90d,
    ABS(prior_rows_60d - 4) AS prior_rows_60d_distance,
    COALESCE(prior_rows_60d, 0) AS prior_rows_60d_filled
FROM scored
WHERE claim_date BETWEEN DATE '2024-03-30' AND DATE '2025-04-25'
ORDER BY claim_date_plus_90d, id NULLS LAST

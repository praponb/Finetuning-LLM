WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.approved_amount) AS prior_approved_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM in_window
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_90d,
    prior_approved_amount_90d
FROM scored
WHERE claim_date_label IS NOT NULL
ORDER BY claim_date DESC

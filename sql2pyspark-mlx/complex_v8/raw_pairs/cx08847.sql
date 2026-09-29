WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
totals AS (
    -- Summarise per group
    SELECT
        prior_rows_30d,
        MAX(prior_approved_amount_30d) AS max_prior_approved_amount_30d,
        SUM(prior_rows_30d) AS total_prior_rows_30d
    FROM matched
    GROUP BY prior_rows_30d
)
SELECT prior_rows_30d
FROM totals
WHERE total_prior_rows_30d <= 29
ORDER BY prior_rows_30d NULLS LAST

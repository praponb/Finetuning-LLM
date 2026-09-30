WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.claimed_amount) AS prior_claimed_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM in_window s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_date >= DATE '2024-04-15'
        )
)
SELECT
    policy_id,
    prior_rows_30d,
    MIN(claim_date) AS first_claim_date,
    COUNT(DISTINCT id) AS distinct_ids
FROM screened
GROUP BY GROUPING SETS ((policy_id, prior_rows_30d), (policy_id), (prior_rows_30d), ())
ORDER BY policy_id NULLS LAST, prior_rows_30d NULLS LAST

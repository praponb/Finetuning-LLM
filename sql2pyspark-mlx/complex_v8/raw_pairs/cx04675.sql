WITH covered AS (
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
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_approved_amount_60d,
    DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
FROM filtered
ORDER BY claim_date, policy_id DESC, id
LIMIT 10

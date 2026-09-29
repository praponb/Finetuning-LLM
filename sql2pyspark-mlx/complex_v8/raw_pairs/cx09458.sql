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
with_activity AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM in_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.premium <= 4678.8
        )
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date < DATE '2025-03-31'
        )
      AND s.prior_claimed_amount_30d >= 100000.0
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_claimed_amount_30d
FROM screened
ORDER BY id
LIMIT 20

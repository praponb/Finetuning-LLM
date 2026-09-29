WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.claimed_amount) AS prior_claimed_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
qualified AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.premium <= 4417.0
        )
),
screened AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.status NOT IN ('ACTIVE', 'CANCELLED')
              AND p.product NOT IN ('HOME', 'HEALTH')
        )
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_60d,
    DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
FROM without_matches
ORDER BY id, policy_id DESC
LIMIT 20

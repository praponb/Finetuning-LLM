WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
tagged AS (
    -- Label and score rows
    SELECT *, YEAR(claim_date) AS claim_date_year
    FROM matched
    WHERE prior_rows_180d < 7
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_claimed_amount_180d,
        s.claim_date_year,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium
    FROM tagged s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium >= 1042.8
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    prior_claimed_amount_180d,
    claim_date_year,
    policy_premium
FROM with_details
WHERE policy_id IS NOT NULL

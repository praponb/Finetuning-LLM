WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_activity AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND UPPER(TRIM(a.adjuster_name)) LIKE 'E%'
              AND a.level BETWEEN 1 AND 3
        )
)
SELECT
    id,
    policy_id,
    claimed_amount,
    adjuster_id,
    claim_type,
    product,
    FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
FROM with_activity
ORDER BY id DESC

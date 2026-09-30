WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id IS NOT NULL
        )
),
filtered AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level >= 1
              AND LOWER(a.adjuster_name) LIKE '%h %'
        )
)
SELECT *
FROM (
    SELECT policy_id, product, premium
    FROM filtered
) src
PIVOT (
    AVG(premium)
    FOR product IN ('HEALTH', 'LIFE', 'HOME')
)

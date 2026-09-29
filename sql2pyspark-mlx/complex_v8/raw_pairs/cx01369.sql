WITH covered AS (
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
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date BETWEEN DATE '2024-03-01' AND DATE '2025-04-20'
              AND c.reported_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_60d,
        s.prior_claimed_amount_60d,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        po.holder_name
    FROM without_matches s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(prior_rows_60d - 5) AS prior_rows_60d_distance,
        SUBSTRING(TRIM(policy_product), 1, 4) AS policy_product_prefix,
        LOWER(REGEXP_REPLACE(TRIM(policy_product), ' ', '_')) AS policy_product_slug
    FROM enriched
    WHERE prior_rows_60d < 5
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_status, policy_id, policy_product, prior_claimed_amount_60d
    FROM derived
) src
PIVOT (
    MIN(prior_claimed_amount_60d)
    FOR policy_product IN ('LIFE', 'AUTO')
)

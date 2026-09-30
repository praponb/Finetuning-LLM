WITH covered AS (
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
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(prior_claimed_amount_30d - 87500.0) AS prior_claimed_amount_30d_distance,
        FLOOR(prior_claimed_amount_30d / 50000) * 50000 AS prior_claimed_amount_30d_bucket,
        ROUND(prior_claimed_amount_30d * 1.5, 2) AS prior_claimed_amount_30d_adj
    FROM covered
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.prior_claimed_amount_30d_distance,
        s.prior_claimed_amount_30d_bucket,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        po.segment AS holder_segment
    FROM scored s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.status IN ('EXPIRED', 'LAPSED')
        )
)
SELECT
    id,
    policy_id,
    prior_rows_30d,
    prior_claimed_amount_30d_distance,
    prior_claimed_amount_30d_bucket,
    policy_end_date,
    policy_premium,
    holder_segment,
    prior_rows_30d + policy_premium AS prior_rows_30d_plus_policy_premium
FROM filtered
ORDER BY id

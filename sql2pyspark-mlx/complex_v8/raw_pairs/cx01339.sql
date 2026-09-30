WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.product,
        s.premium,
        p.end_date AS policy_end_date
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date >= DATE '2026-01-24'
),
latest AS (
    -- Keep the latest row per product, status
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        status,
        product,
        premium,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY claimed_amount DESC, id DESC) AS rn
        FROM with_details_policy
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    approved_amount,
    status,
    premium,
    policy_end_date,
    ABS(approved_amount - 4554.8) AS approved_amount_distance,
    YEAR(policy_end_date) AS policy_end_date_year
FROM latest
ORDER BY policy_end_date_year DESC

WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(claim_date) AS claim_date_year,
        policy_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        ROUND(AVG(premium), 2) AS avg_premium,
        MIN(claim_date) AS first_claim_date,
        SUM(claimed_amount) FILTER (WHERE product IN ('LIFE', 'HEALTH')) AS claimed_amount_product_in_life_health,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days
    FROM covered
    GROUP BY YEAR(claim_date), policy_id
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.claim_date_year,
        s.policy_id,
        s.first_claim_date,
        s.claimed_amount_product_in_life_health,
        p.status AS policy_status
    FROM rollup_base s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    claim_date_year,
    policy_id,
    first_claim_date,
    claimed_amount_product_in_life_health,
    policy_status,
    COALESCE(policy_status, 'UNKNOWN') AS policy_status_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(policy_status), ' ', '_')) AS policy_status_slug
FROM with_parent
WHERE claimed_amount_product_in_life_health < 187500.0
ORDER BY policy_status_or_unknown, first_claim_date DESC, claim_date_year, policy_id
LIMIT 5

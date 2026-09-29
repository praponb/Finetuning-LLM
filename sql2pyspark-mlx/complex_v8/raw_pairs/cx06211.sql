WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY status ORDER BY premium, id) AS claimed_amount_change
    FROM matched
),
latest AS (
    -- Keep the latest row per claim_type, status
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        status,
        product,
        premium,
        policy_id_sum_claimed_amount,
        claimed_amount_change
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY claimed_amount_change DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    claim_date,
    claim_type,
    product,
    policy_id_sum_claimed_amount,
    SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix,
    CASE WHEN claimed_amount_change >= 20000.0 THEN 'HIGH' WHEN claimed_amount_change >= -25000.0 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_change_tier
FROM latest
ORDER BY claimed_amount_change_tier

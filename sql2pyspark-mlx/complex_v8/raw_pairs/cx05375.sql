WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type,
        s.status,
        s.product,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date < DATE '2026-11-25'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        policy_id,
        ROUND(SUM(policy_premium), 2) AS total_policy_premium,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN claim_type IN ('delay', 'flood', 'theft') THEN 1 END) / COUNT(*), 1) AS pct_claim_type_in_delay_flood_th
    FROM joined_policy
    GROUP BY claim_type, policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT claim_type, policy_id, metric_name, reading
FROM (
    SELECT claim_type, policy_id, pct_claim_type_in_delay_flood_th, total_policy_premium
    FROM stats
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (pct_claim_type_in_delay_flood_th, total_policy_premium)
) unp

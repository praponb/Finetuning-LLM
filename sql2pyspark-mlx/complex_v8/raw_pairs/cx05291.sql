WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.total_claim_approved_amount
    FROM covered s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, product, premium
    FROM summarized_rows
) src
PIVOT (
    COUNT(premium)
    FOR product IN ('AUTO', 'HEALTH')
)
ORDER BY adjuster_id

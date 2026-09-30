WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount
    FROM matched s
)
SELECT id, metric, metric_value
FROM (
    SELECT id, avg_claim_approved_amount, premium
    FROM with_stats
) src
UNPIVOT (
    metric_value FOR metric IN (avg_claim_approved_amount, premium)
) unp
ORDER BY id, metric

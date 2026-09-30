WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_approved_amount_30d,
        p.status AS policy_status
    FROM covered s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date
    FROM joined_policy s
)
SELECT *
FROM (
    SELECT claim_date, last_claim_date, policy_status, prior_approved_amount_30d
    FROM with_counts
) src
PIVOT (
    SUM(prior_approved_amount_30d)
    FOR policy_status IN ('ACTIVE', 'EXPIRED')
)
ORDER BY claim_date, last_claim_date

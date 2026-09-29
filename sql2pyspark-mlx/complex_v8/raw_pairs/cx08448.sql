WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM in_window s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
        )
      AND s.prior_rows_180d > 3
),
joined AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_claimed_amount_180d,
        p.status AS policy_status,
        po.joined_on AS holder_joined_on,
        po.risk_score AS holder_risk_score
    FROM screened s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    claim_date,
    policy_status,
    holder_risk_score,
    COALESCE(holder_risk_score, 0.0) AS holder_risk_score_filled,
    SUBSTRING(TRIM(policy_status), 1, 2) AS policy_status_prefix
FROM joined
WHERE holder_risk_score >= 0.1
ORDER BY holder_risk_score DESC

WITH in_window AS (
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
policy_id_stats AS (
    -- Avg prior_rows_30d per policy_id
    SELECT policy_id, AVG(prior_rows_30d) AS avg_prior_rows_30d
    FROM in_window
    GROUP BY policy_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_30d,
        ROUND(s.prior_rows_30d - g.avg_prior_rows_30d, 2) AS prior_rows_30d_minus_avg
    FROM in_window s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_30d > g.avg_prior_rows_30d
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.prior_claimed_amount_30d,
        s.avg_prior_rows_30d,
        s.prior_rows_30d_minus_avg,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        po.risk_score AS holder_risk_score
    FROM outliers s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_30d,
    avg_prior_rows_30d,
    prior_rows_30d_minus_avg,
    policy_holder_id,
    policy_end_date,
    holder_risk_score,
    TRUNC(claim_date, 'MM') AS claim_date_month_start
FROM enriched
WHERE policy_id IS NOT NULL
ORDER BY prior_rows_30d_minus_avg

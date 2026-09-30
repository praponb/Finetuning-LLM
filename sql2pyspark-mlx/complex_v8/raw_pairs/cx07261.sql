WITH matched AS (
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
policy_id_stats AS (
    -- Avg prior_rows_30d per policy_id
    SELECT policy_id, AVG(prior_rows_30d) AS avg_prior_rows_30d
    FROM matched
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_30d,
        ROUND(s.prior_rows_30d - g.avg_prior_rows_30d, 2) AS prior_rows_30d_minus_avg
    FROM matched s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_30d > g.avg_prior_rows_30d
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_30d,
    prior_approved_amount_30d,
    ROUND(avg_prior_rows_30d / NULLIF(prior_rows_30d, 0), 3) AS avg_prior_rows_30d_per_prior_rows_30d,
    DATE_ADD(claim_date, 30) AS claim_date_plus_30d
FROM compared

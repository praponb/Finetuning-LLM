WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.approved_amount) AS prior_approved_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
derived AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM matched
),
policy_id_stats AS (
    -- Median prior_rows_90d per policy_id
    SELECT policy_id, PERCENTILE_APPROX(prior_rows_90d, 0.5) AS median_prior_rows_90d
    FROM derived
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_90d,
        ROUND(s.prior_rows_90d - g.median_prior_rows_90d, 2) AS prior_rows_90d_minus_median
    FROM derived s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_90d >= g.median_prior_rows_90d
)
-- Final projection
SELECT
    id,
    claim_date,
    prior_approved_amount_90d,
    claim_date_week,
    median_prior_rows_90d,
    prior_rows_90d_minus_median,
    COALESCE(prior_rows_90d, 0) AS prior_rows_90d_filled
FROM compared
WHERE median_prior_rows_90d >= 2.4

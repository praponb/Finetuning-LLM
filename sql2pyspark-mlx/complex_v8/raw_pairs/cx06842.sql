WITH matched AS (
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
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        prior_rows_180d,
        policy_id,
        ROUND(STDDEV(prior_claimed_amount_180d), 2) AS stddev_prior_claimed_amount_180d,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        PERCENTILE_APPROX(prior_rows_180d, 0.5) AS median_prior_rows_180d,
        MIN(claim_date) AS first_claim_date,
        ROUND(STDDEV(prior_rows_180d), 2) AS stddev_prior_rows_180d
    FROM matched
    GROUP BY DATE_TRUNC('MONTH', claim_date), prior_rows_180d, policy_id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(median_prior_rows_180d / NULLIF(prior_rows_180d, 0), 3) AS median_prior_rows_180d_per_prior_rows_180d
    FROM stats
)
SELECT
    claim_date_month,
    policy_id,
    stddev_prior_claimed_amount_180d,
    claim_date_span_days,
    median_prior_rows_180d,
    first_claim_date,
    stddev_prior_rows_180d,
    median_prior_rows_180d_per_prior_rows_180d,
    TO_DATE(claim_date_month) AS claim_date_month_date
FROM scored
WHERE median_prior_rows_180d_per_prior_rows_180d < 8.5
ORDER BY policy_id DESC

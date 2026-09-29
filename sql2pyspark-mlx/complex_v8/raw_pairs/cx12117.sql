WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.claimed_amount) AS prior_claimed_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN prior_claimed_amount_90d >= 175000.0 THEN 'large' WHEN prior_claimed_amount_90d >= 62500.0 THEN 'medium' ELSE 'small' END AS prior_claimed_amount_90d_tier,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        ADD_MONTHS(claim_date, 12) AS claim_date_plus_12m
    FROM covered
    WHERE policy_id >= 5
),
totals AS (
    -- Group-level totals
    SELECT
        prior_rows_90d,
        DATE_TRUNC('MONTH', claim_date_plus_12m) AS claim_date_plus_12m_month,
        prior_claimed_amount_90d_tier,
        COUNT(*) AS cnt,
        MAX(prior_rows_90d) AS max_prior_rows_90d,
        MIN(prior_rows_90d) AS min_prior_rows_90d,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        ROUND(STDDEV(prior_claimed_amount_90d), 2) AS stddev_prior_claimed_amount_90d
    FROM derived
    WHERE prior_claimed_amount_90d >= 112500.0
    GROUP BY prior_rows_90d, DATE_TRUNC('MONTH', claim_date_plus_12m), prior_claimed_amount_90d_tier
    HAVING SUM(prior_rows_90d) > 2
)
-- Final projection
SELECT
    prior_rows_90d,
    claim_date_plus_12m_month,
    prior_claimed_amount_90d_tier,
    max_prior_rows_90d,
    distinct_policy_ids,
    max_prior_rows_90d - min_prior_rows_90d AS max_prior_rows_90d_minus_min_prior_rows_90d
FROM totals
WHERE min_prior_rows_90d <= 5
ORDER BY prior_claimed_amount_90d_tier

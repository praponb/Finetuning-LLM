WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.approved_amount) AS prior_approved_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_60d,
        prior_approved_amount_60d,
        ROUND(AVG(prior_approved_amount_60d) OVER (PARTITION BY policy_id ORDER BY prior_rows_60d, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS prior_approved_amount_60d_avg_3,
        ROUND(prior_rows_60d - AVG(prior_rows_60d) OVER (PARTITION BY prior_rows_60d), 2) AS prior_rows_60d_vs_prior_rows_60d_avg,
        LAG(prior_rows_60d, 2) OVER (PARTITION BY policy_id ORDER BY prior_rows_60d, id) AS prev2_prior_rows_60d
    FROM covered
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_60d,
    prior_approved_amount_60d,
    prior_rows_60d_vs_prior_rows_60d_avg,
    prev2_prior_rows_60d,
    CASE WHEN prev2_prior_rows_60d >= 5 THEN 'large' WHEN prev2_prior_rows_60d >= 2 THEN 'medium' ELSE 'small' END AS prev2_prior_rows_60d_tier,
    LAST_DAY(claim_date) AS claim_date_month_end
FROM windowed
ORDER BY prior_rows_60d, prev2_prior_rows_60d_tier DESC NULLS FIRST, id
LIMIT 5

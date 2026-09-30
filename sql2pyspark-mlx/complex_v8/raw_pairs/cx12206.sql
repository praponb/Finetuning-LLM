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
per_group AS (
    -- Group-level totals
    SELECT
        prior_rows_30d,
        SUM(prior_rows_30d) AS total_prior_rows_30d,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(prior_approved_amount_30d, 0.5) AS median_prior_approved_amount_30d,
        COUNT(DISTINCT id) AS distinct_ids
    FROM covered
    GROUP BY 1
)
SELECT
    prior_rows_30d,
    n_rows,
    median_prior_approved_amount_30d,
    distinct_ids,
    ABS(prior_rows_30d - 4) AS prior_rows_30d_distance
FROM per_group
ORDER BY n_rows DESC NULLS FIRST, prior_rows_30d_distance

WITH joined_ranges AS (
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
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_60d,
        policy_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) AS n_rows
    FROM joined_ranges
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
)
SELECT
    n_rows,
    prior_rows_60d,
    MIN(last_claim_date) AS first_last_claim_date,
    COUNT(*) AS row_count,
    ROUND(STDDEV(n_rows), 2) AS stddev_n_rows
FROM summary
GROUP BY ROLLUP(n_rows, prior_rows_60d)

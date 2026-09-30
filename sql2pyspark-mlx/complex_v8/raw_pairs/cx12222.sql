WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.approved_amount) AS prior_approved_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        prior_rows_180d,
        ROUND(SUM(prior_approved_amount_180d) / NULLIF(SUM(prior_rows_180d), 0), 3) AS prior_approved_amount_180d_to_prior_rows_180d_ra,
        MIN(claim_date) AS first_claim_date,
        MIN(prior_approved_amount_180d) AS min_prior_approved_amount_180d,
        ROUND(AVG(prior_rows_180d), 2) AS avg_prior_rows_180d,
        ROUND(AVG(prior_approved_amount_180d), 2) AS avg_prior_approved_amount_180d
    FROM matched
    WHERE claim_date BETWEEN DATE '2024-06-13' AND DATE '2025-05-31'
    GROUP BY policy_id, prior_rows_180d
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(first_claim_date, 'yyyy-MM') AS first_claim_date_label,
        DATE_ADD(first_claim_date, 365) AS first_claim_date_plus_365d
    FROM summary
)
SELECT
    policy_id,
    prior_rows_180d,
    prior_approved_amount_180d_to_prior_rows_180d_ra,
    first_claim_date,
    min_prior_approved_amount_180d,
    avg_prior_approved_amount_180d,
    first_claim_date_label,
    first_claim_date_plus_365d,
    DATEDIFF(DATE '2025-12-31', first_claim_date) AS days_since_first_claim_date
FROM annotated
ORDER BY avg_prior_approved_amount_180d DESC, policy_id DESC

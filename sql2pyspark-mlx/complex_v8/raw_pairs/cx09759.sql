WITH in_window AS (
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
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_30d,
        prior_approved_amount_30d,
        ABS(prior_rows_30d - 4) AS prior_rows_30d_distance,
        ABS(prior_rows_30d - 4) AS prior_rows_30d_distance_2,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM in_window
    WHERE policy_id IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_30d_distance,
        MAX(claim_date) AS last_claim_date,
        MIN(claim_date) AS first_claim_date,
        ROUND(SUM(prior_rows_30d_distance) / NULLIF(SUM(prior_rows_30d), 0), 3) AS prior_rows_30d_distance_to_prior_rows_30d_ratio,
        DATEDIFF(MAX(claim_date_month_end), MIN(claim_date_month_end)) AS claim_date_month_end_span_days,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM derived
    GROUP BY prior_rows_30d_distance
)
-- Final projection
SELECT
    prior_rows_30d_distance,
    last_claim_date,
    first_claim_date,
    prior_rows_30d_distance_to_prior_rows_30d_ratio,
    claim_date_month_end_span_days,
    distinct_policy_ids
FROM totals
ORDER BY prior_rows_30d_distance_to_prior_rows_30d_ratio DESC, first_claim_date DESC

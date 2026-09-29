WITH in_window AS (
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
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(prior_rows_90d / NULLIF(SUM(prior_rows_90d) OVER w_prior_rows_90d, 0), 4) AS prior_rows_90d_share_of_prior_rows_90d,
        ROUND(AVG(prior_claimed_amount_90d) OVER (PARTITION BY prior_rows_90d), 2) AS prior_rows_90d_avg_prior_claimed_amount_90d,
        ROUND(prior_claimed_amount_90d / NULLIF(SUM(prior_claimed_amount_90d) OVER (PARTITION BY prior_rows_90d), 0), 4) AS prior_claimed_amount_90d_share_of_prior_rows_90d,
        MAX(prior_claimed_amount_90d) OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prior_claimed_amount_90d_max_6_prior,
        NTH_VALUE(prior_rows_90d, 2) OVER (ORDER BY prior_rows_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_90d
    FROM in_window
    WINDOW w_prior_rows_90d AS (PARTITION BY prior_rows_90d)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label_2,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM ranked
    WHERE prior_claimed_amount_90d_max_6_prior >= 75000.0
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE second_prior_rows_90d > (SELECT MIN(second_prior_rows_90d) FROM tagged)
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_90d,
    prior_claimed_amount_90d,
    prior_rows_90d_share_of_prior_rows_90d,
    prior_rows_90d_avg_prior_claimed_amount_90d,
    prior_claimed_amount_90d_share_of_prior_rows_90d,
    claim_date_label,
    claim_date_label_2,
    SUBSTRING(TRIM(claim_date_label), 1, 3) AS claim_date_label_prefix,
    ROUND(prior_rows_90d / NULLIF(second_prior_rows_90d, 0), 3) AS prior_rows_90d_per_second_prior_rows_90d
FROM without_matches
WHERE prior_claimed_amount_90d_share_of_prior_rows_90d > 0.3
ORDER BY prior_claimed_amount_90d_share_of_prior_rows_90d NULLS LAST, claim_date_label DESC

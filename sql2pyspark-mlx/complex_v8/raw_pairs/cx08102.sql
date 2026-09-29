WITH matched AS (
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
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        prior_approved_amount_60d,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM matched
    WHERE policy_id >= 11
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_prior_approved_amount, 3) AS prior_approved_amount_60d_cume_dist,
        ROUND(prior_approved_amount_60d - AVG(prior_approved_amount_60d) OVER w_claim_date_year, 2) AS prior_approved_amount_60d_vs_claim_date_year_avg,
        MIN(prior_approved_amount_60d) OVER (PARTITION BY YEAR(claim_date)) AS claim_date_year_min_prior_approved_amount_60d,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY prior_approved_amount_60d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(prior_approved_amount_60d / NULLIF(SUM(prior_approved_amount_60d) OVER w_claim_date_year, 0), 4) AS prior_approved_amount_60d_share_of_claim_date_ye
    FROM annotated
    WINDOW w_all_by_prior_approved_amount AS (ORDER BY prior_approved_amount_60d DESC), w_claim_date_year AS (PARTITION BY YEAR(claim_date))
)
SELECT
    id,
    claim_date,
    claim_date_label,
    prior_approved_amount_60d_cume_dist,
    prior_approved_amount_60d_vs_claim_date_year_avg,
    second_claim_date,
    prior_approved_amount_60d_share_of_claim_date_ye,
    DATEDIFF(claim_date, second_claim_date) AS days_second_claim_date_to_claim_date
FROM metrics
WHERE second_claim_date BETWEEN DATE '2024-02-23' AND DATE '2025-08-10'
ORDER BY prior_approved_amount_60d_cume_dist DESC NULLS FIRST, second_claim_date DESC NULLS FIRST

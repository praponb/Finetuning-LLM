WITH covered AS (
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
policy_id_stats AS (
    -- Avg prior_rows_180d per policy_id
    SELECT policy_id, AVG(prior_rows_180d) AS avg_prior_rows_180d
    FROM covered
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_180d,
        ROUND(s.prior_rows_180d - g.avg_prior_rows_180d, 2) AS prior_rows_180d_minus_avg
    FROM covered s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_180d > g.avg_prior_rows_180d
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id) AS claim_date_quartile,
        MIN(prior_rows_180d_minus_avg) OVER w_claim_date_year AS claim_date_year_min_prior_rows_180d_minus_avg,
        NTILE(10) OVER w_claim_date_year_by_claim_dat AS claim_date_decile,
        ROUND(AVG(prior_rows_180d) OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS prior_rows_180d_avg_6_prior,
        SUM(prior_rows_180d) OVER w_claim_date_year AS claim_date_year_sum_prior_rows_180d
    FROM above_average
    WINDOW w_claim_date_year AS (PARTITION BY YEAR(claim_date)), w_claim_date_year_by_claim_dat AS (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    avg_prior_rows_180d,
    claim_date_year_min_prior_rows_180d_minus_avg,
    claim_date_decile,
    prior_rows_180d_avg_6_prior,
    claim_date_year_sum_prior_rows_180d
FROM ranked
WHERE prior_approved_amount_180d < 225000.0
ORDER BY claim_date_year_sum_prior_rows_180d, claim_date_year_min_prior_rows_180d_minus_avg, id
LIMIT 20

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
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(claim_date) OVER (PARTITION BY policy_id, prior_rows_90d ORDER BY prior_claimed_amount_90d, id) AS prev_claim_date,
        ROUND(prior_rows_90d - AVG(prior_rows_90d) OVER w_prior_rows_90d, 2) AS prior_rows_90d_vs_prior_rows_90d_avg,
        ROUND(prior_claimed_amount_90d / NULLIF(SUM(prior_claimed_amount_90d) OVER w_prior_rows_90d, 0), 4) AS prior_claimed_amount_90d_share_of_prior_rows_90d,
        NTH_VALUE(prior_claimed_amount_90d, 2) OVER (PARTITION BY policy_id, prior_rows_90d ORDER BY prior_claimed_amount_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_claimed_amount_90d
    FROM covered
    WINDOW w_prior_rows_90d AS (PARTITION BY prior_rows_90d)
)
SELECT DISTINCT id, prior_claimed_amount_90d
FROM windowed
ORDER BY id DESC

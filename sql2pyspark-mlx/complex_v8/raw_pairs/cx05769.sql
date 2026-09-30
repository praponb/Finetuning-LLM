WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_180d,
        prior_claimed_amount_180d,
        prior_rows_180d - LAG(prior_rows_180d) OVER (PARTITION BY prior_rows_180d ORDER BY prior_claimed_amount_180d NULLS LAST, id) AS prior_rows_180d_change,
        NTH_VALUE(prior_rows_180d, 2) OVER (PARTITION BY prior_rows_180d ORDER BY prior_claimed_amount_180d NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_180d,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY prior_rows_180d ORDER BY prior_claimed_amount_180d NULLS LAST, id)) AS days_since_prev_claim_date,
        MAX(prior_claimed_amount_180d) OVER w_policy_id_prior_rows_180d AS policy_id_prior_rows_180d_max_prior_claimed_amou
    FROM matched
    WINDOW w_policy_id_prior_rows_180d AS (PARTITION BY policy_id, prior_rows_180d)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        prior_rows_180d,
        prior_claimed_amount_180d,
        RANK() OVER (ORDER BY prior_claimed_amount_180d ASC) AS prior_claimed_amount_180d_rank,
        COUNT(*) OVER (PARTITION BY second_prior_rows_180d) AS rows_in_second_prior_rows_180d
    FROM ranked
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        prior_rows_180d,
        prior_claimed_amount_180d,
        rows_in_second_prior_rows_180d,
        COALESCE(prior_rows_180d, 0) AS prior_rows_180d_filled
    FROM metrics
)
-- Final projection
SELECT
    id,
    policy_id,
    prior_claimed_amount_180d,
    prior_rows_180d_filled
FROM derived
ORDER BY policy_id, id
LIMIT 5

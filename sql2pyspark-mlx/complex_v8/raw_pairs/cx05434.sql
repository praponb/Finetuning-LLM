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
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(prior_rows_180d) OVER (PARTITION BY prior_rows_180d) AS prior_rows_180d_max_prior_rows_180d,
        ROUND(prior_rows_180d - AVG(prior_rows_180d) OVER (PARTITION BY prior_rows_180d), 2) AS prior_rows_180d_vs_prior_rows_180d_avg,
        MAX(prior_rows_180d) OVER (PARTITION BY policy_id ORDER BY prior_rows_180d, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prior_rows_180d_max_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY claim_date), 3) AS claim_date_cume_dist,
        SUM(prior_claimed_amount_180d) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prior_claimed_amount_180d
    FROM with_activity
),
policy_id_stats AS (
    -- Avg prior_rows_180d per policy_id
    SELECT policy_id, AVG(prior_rows_180d) AS avg_prior_rows_180d
    FROM sequenced
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_180d,
        ROUND(s.prior_rows_180d - g.avg_prior_rows_180d, 2) AS prior_rows_180d_minus_avg
    FROM sequenced s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_180d > g.avg_prior_rows_180d
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    prior_claimed_amount_180d,
    prior_rows_180d_max_prior_rows_180d,
    prior_rows_180d_vs_prior_rows_180d_avg,
    prior_rows_180d_max_1_prior,
    running_prior_claimed_amount_180d,
    avg_prior_rows_180d,
    prior_rows_180d_minus_avg
FROM vs_group
WHERE prior_rows_180d_max_1_prior <= 14
ORDER BY prior_rows_180d_max_prior_rows_180d DESC

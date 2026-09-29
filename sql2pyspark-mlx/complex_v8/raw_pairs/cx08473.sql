WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.claimed_amount) AS prior_claimed_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MAX(claim_date) AS last_claim_date, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM covered s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(prior_rows_30d), 2) AS stddev_prior_rows_30d
    FROM with_counts
    WHERE claim_count > 4
    GROUP BY policy_id
)
-- Final projection
SELECT
    policy_id,
    n_rows,
    stddev_prior_rows_30d + n_rows AS stddev_prior_rows_30d_plus_n_rows,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket
FROM summary
ORDER BY n_rows DESC NULLS FIRST, stddev_prior_rows_30d_plus_n_rows DESC NULLS FIRST, policy_id
LIMIT 20

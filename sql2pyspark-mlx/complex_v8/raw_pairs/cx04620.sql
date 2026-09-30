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
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM covered s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.adjuster_id <= 2
              AND c.claimed_amount < 31752.9
        )
),
policy_id_stats AS (
    -- Median prior_claimed_amount_30d per policy_id
    SELECT policy_id, PERCENTILE_APPROX(prior_claimed_amount_30d, 0.5) AS median_prior_claimed_amount_30d
    FROM qualified
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_claimed_amount_30d,
        ROUND(s.prior_claimed_amount_30d - g.median_prior_claimed_amount_30d, 2) AS prior_claimed_amount_30d_minus_median
    FROM qualified s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_claimed_amount_30d >= g.median_prior_claimed_amount_30d
)
SELECT id, policy_id, prior_rows_30d
FROM vs_group
ORDER BY policy_id DESC, id

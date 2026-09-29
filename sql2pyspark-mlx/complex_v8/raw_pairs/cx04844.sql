WITH joined_ranges AS (
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
policy_id_stats AS (
    -- Avg prior_claimed_amount_90d per policy_id
    SELECT policy_id, AVG(prior_claimed_amount_90d) AS avg_prior_claimed_amount_90d
    FROM joined_ranges
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_claimed_amount_90d,
        ROUND(s.prior_claimed_amount_90d - g.avg_prior_claimed_amount_90d, 2) AS prior_claimed_amount_90d_minus_avg
    FROM joined_ranges s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_claimed_amount_90d > g.avg_prior_claimed_amount_90d
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND MONTH(c.reported_at) IN (1, 2)
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_90d,
        s.prior_claimed_amount_90d_minus_avg,
        p.holder_id AS policy_holder_id
    FROM with_activity s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_90d
FROM joined
ORDER BY id DESC NULLS FIRST

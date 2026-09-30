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
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_claimed_amount
    FROM matched s
),
policy_id_stats AS (
    -- Avg prior_approved_amount_60d per policy_id
    SELECT policy_id, AVG(prior_approved_amount_60d) AS avg_prior_approved_amount_60d
    FROM profiled
    GROUP BY policy_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_approved_amount_60d,
        ROUND(s.prior_approved_amount_60d - g.avg_prior_approved_amount_60d, 2) AS prior_approved_amount_60d_minus_avg
    FROM profiled s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_approved_amount_60d > g.avg_prior_approved_amount_60d
)
SELECT id, measure, value
FROM (
    SELECT id, avg_claim_claimed_amount, avg_prior_approved_amount_60d
    FROM outliers
) src
UNPIVOT (
    value FOR measure IN (avg_claim_claimed_amount, avg_prior_approved_amount_60d)
) unp
ORDER BY id, measure

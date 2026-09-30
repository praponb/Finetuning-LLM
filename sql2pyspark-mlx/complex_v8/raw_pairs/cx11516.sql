WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
policy_id_stats AS (
    -- Median prior_rows_30d per policy_id
    SELECT policy_id, PERCENTILE_APPROX(prior_rows_30d, 0.5) AS median_prior_rows_30d
    FROM in_window
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_30d,
        ROUND(s.prior_rows_30d - g.median_prior_rows_30d, 2) AS prior_rows_30d_minus_median
    FROM in_window s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_30d >= g.median_prior_rows_30d
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM vs_group s
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.prior_rows_30d,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type,
        c.reported_at AS claim_reported_at
    FROM with_counts s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    prior_rows_30d,
    claim_status,
    claim_adjuster_id,
    claim_type,
    claim_reported_at
FROM exploded_detail
ORDER BY claim_status DESC

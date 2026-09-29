WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 6 AND 8
),
policy_id_stats AS (
    -- Median approved_amount per policy_id
    SELECT policy_id, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM valid_claims
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM valid_claims s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount >= g.median_approved_amount
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level BETWEEN 2 AND 3
        )
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, approved_amount, median_approved_amount, approved_amount_minus_median
    FROM qualified
) src
UNPIVOT (
    metric_value FOR metric_name IN (approved_amount AS approved_amount_v, median_approved_amount AS m_median_approved_amount, approved_amount_minus_median AS approved_amount_minus_median_v)
) unp

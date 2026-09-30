WITH cleaned_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policyholders
    WHERE joined_on >= DATE '2024-06-29'
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM cleaned_holders s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium <= 4670.9
              AND p.start_date < DATE '2025-05-02'
        )
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MAX(end_date) AS last_policy_end_date,
        COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.last_policy_end_date,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM with_activity s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        ROUND(SUM(risk_score) / NULLIF(SUM(policy_count), 0), 3) AS risk_score_to_policy_count_ratio,
        MAX(policy_count) AS max_policy_count
    FROM with_stats
    GROUP BY region
    HAVING SUM(risk_score) > 0.2
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM summary
    WHERE max_policy_count < (SELECT MAX(max_policy_count) FROM summary)
)
SELECT region, max_policy_count
FROM without_matches

WITH scoped AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        CONCAT_WS(' / ', holder_name, segment) AS holder_name_segment_label
    FROM policyholders
    WHERE segment IS NOT NULL
      AND region IS NOT NULL
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_end_date
    FROM scoped s
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT holder_id, MIN(premium) AS min_policy_premium, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.min_policy_premium,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM summarized_rows s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.id
),
enriched AS (
    -- Label and score rows
    SELECT *, FLOOR(min_policy_premium / 1000) * 1000 AS min_policy_premium_bucket
    FROM profiled
)
SELECT
    policy_count,
    COALESCE(segment, 'ALL') AS segment,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    MAX(risk_score) AS max_risk_score,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY CUBE(policy_count, segment)
ORDER BY policy_count NULLS LAST, segment NULLS LAST

WITH valid_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        CASE WHEN segment IN ('RETAIL', 'VIP') THEN 'group_a' WHEN segment = 'SME' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
    WHERE risk_score >= 0.3
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM valid_holders s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) AS policy_count_2,
        COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS policy_status_count,
        COUNT(*) AS policy_count_3
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count_2, 0) AS policy_count_2,
        COALESCE(p.policy_status_count, 0) AS policy_status_count,
        COALESCE(p.policy_count_3, 0) AS policy_count_3
    FROM with_counts s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', region, segment) AS region_segment_label,
        ROUND(policy_count_2 / NULLIF(policy_count_3, 0), 3) AS policy_count_2_per_policy_count_3,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label
    FROM with_stats
)
SELECT *
FROM (
    SELECT segment_group, policy_count_2, segment, policy_count
    FROM scored
) src
PIVOT (
    COUNT(policy_count)
    FOR segment IN ('VIP', 'SME', 'RETAIL')
)

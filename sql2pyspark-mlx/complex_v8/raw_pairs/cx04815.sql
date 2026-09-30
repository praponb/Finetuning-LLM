WITH cleaned AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE segment = 'SME'
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium <= 3544.1
        )
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, COUNT(*) AS policy_count_2
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2
    FROM filtered s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, policy_count, policy_count_2
    FROM with_stats
) src
UNPIVOT (
    value FOR metric IN (policy_count, policy_count_2)
) unp

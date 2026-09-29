WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_filled,
        p.holder_name
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) AS policy_count,
        COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS policy_status_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_status_count, 0) AS policy_status_count
    FROM with_details s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT
    holder_id,
    status,
    COUNT(*) AS n_rows
FROM with_stats
GROUP BY CUBE(holder_id, status)
ORDER BY holder_id NULLS LAST, status NULLS LAST

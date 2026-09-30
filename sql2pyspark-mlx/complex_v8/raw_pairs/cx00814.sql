WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE priority = 'P4'
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team, su.agent_name
    FROM sessionized s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team IS NOT NULL
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    channel,
    channel_slug,
    prev_closed_at,
    is_new_session,
    agent_team,
    DATE_TRUNC('MONTH', closed_at) AS closed_at_month
FROM enriched
ORDER BY agent_team, channel

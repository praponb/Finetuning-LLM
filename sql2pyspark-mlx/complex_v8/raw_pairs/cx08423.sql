WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'refund')
      AND csat BETWEEN 2 AND 4
),
channel_or_unknown_stats AS (
    -- Max csat per channel_or_unknown
    SELECT channel_or_unknown, MAX(csat) AS max_csat
    FROM base
    GROUP BY channel_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM base s
    JOIN channel_or_unknown_stats g ON g.channel_or_unknown = s.channel_or_unknown
    WHERE s.csat = g.max_csat
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.hours_closed_at_to_opened_at,
        s.max_csat,
        s.csat_minus_max,
        su.team AS agent_team
    FROM outliers s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT priority, agent_id, agent_team, csat
    FROM combined_details
) src
PIVOT (
    MIN(csat) AS min_v, COUNT(csat) AS n
    FOR agent_team IN ('Tech', 'VIP')
)

WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        HOUR(closed_at) AS closed_at_hour
    FROM tickets
    WHERE csat < 4
      AND priority IN ('P1', 'P4')
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags,
        s.closed_at_hour,
        su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.closed_at_hour,
        s.agent_hired_on,
        t.tag
    FROM combined_details_agent s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT tag, agent_hired_on, channel, closed_at_hour
    FROM per_label
) src
PIVOT (
    COUNT(closed_at_hour)
    FOR channel IN ('web', 'email', 'phone', 'chat')
)

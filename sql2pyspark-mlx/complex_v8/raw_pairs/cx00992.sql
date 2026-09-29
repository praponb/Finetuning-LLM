WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        UPPER(TRIM(channel)) AS channel_clean,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE csat >= 2
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.priority_or_unknown,
        s.channel_clean,
        s.csat_filled,
        su.agent_name
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 4
)
SELECT id, field, value
FROM (
    SELECT id, csat, csat_filled
    FROM enriched_agent
) src
UNPIVOT (
    value FOR field IN (csat, csat_filled)
) unp

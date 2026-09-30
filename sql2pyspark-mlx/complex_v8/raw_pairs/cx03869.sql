WITH prepared AS (
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
        tags
    FROM tickets
    WHERE (SIZE(tags) >= 1 OR opened_at < TIMESTAMP '2025-02-18 09:00:00')
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY csat DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM latest s
    JOIN support_agents su ON su.id = s.agent_id
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_details_agent s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type IN ('assigned', 'resolved')
        )
      AND s.opened_at >= TIMESTAMP '2025-01-13 07:00:00'
)
SELECT id, field, value
FROM (
    SELECT id, requester_id, csat
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (requester_id AS requester_id_v, csat AS m_csat)
) unp
ORDER BY id, field

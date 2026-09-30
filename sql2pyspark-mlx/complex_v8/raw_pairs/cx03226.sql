WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 3)
      AND event_type IN ('escalated', 'resolved')
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_details_ticket s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-02-12 12:00:00'
              AND t.actor = 'system'
        )
),
latest_per_key AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        ticket_channel,
        ticket_requester_id,
        agent_team,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY agent_hired_on DESC, id DESC) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT ticket_id, event_type, ticket_requester_id
    FROM latest_per_key
) src
PIVOT (
    MAX(ticket_requester_id)
    FOR event_type IN ('escalated', 'replied')
)

WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE event_type IN ('resolved', 'assigned')
      AND actor <> 'customer'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_closed_at,
        agent_tier,
        agent_team,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(ticket_closed_at)) / 3600, 1) AS hours_ticket_closed_at_to_event_at
    FROM with_details_ticket
)
SELECT
    actor,
    agent_team,
    agent_tier,
    ROUND(AVG(hours_ticket_closed_at_to_event_at), 2) AS avg_hours_ticket_closed_at_to_event_at
FROM enriched
GROUP BY ROLLUP(actor, agent_team, agent_tier)
ORDER BY actor NULLS LAST, agent_team NULLS LAST, agent_tier NULLS LAST

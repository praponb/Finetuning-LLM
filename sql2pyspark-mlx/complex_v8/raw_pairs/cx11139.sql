WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_date,
        YEAR(event_at) AS event_at_year,
        DATE_TRUNC('YEAR', event_at_date) AS event_at_date_year,
        MONTH(event_at) AS event_at_month
    FROM prepared_events
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_year,
        s.event_at_date_year,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.opened_at < TIMESTAMP '2025-03-08 11:00:00'
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_year,
    event_at_date_year,
    ticket_closed_at,
    ticket_channel,
    agent_tier,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    DATE_FORMAT(ticket_closed_at, 'yyyy-MM-dd') AS ticket_closed_at_label
FROM joined
ORDER BY event_at_year DESC, actor

WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_year,
        s.event_type_slug,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel,
        su.team AS agent_team,
        su.agent_name
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.priority NOT IN ('P4', 'P3')
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_at_year,
        s.event_type_slug,
        s.ticket_priority,
        s.ticket_channel,
        s.agent_team,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM with_parent_ticket s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM detail_rows_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND s.ticket_id <= 47
),
latest AS (
    -- Keep the latest row per agent_team
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_year,
        event_type_slug,
        ticket_priority,
        ticket_channel,
        agent_team,
        event_id,
        event_event_type,
        event_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team ORDER BY event_at_year DESC, id DESC, event_id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at_year,
        s.ticket_priority,
        s.ticket_channel,
        s.event_id,
        s.event_event_at,
        t.actor AS event_actor
    FROM latest s
    JOIN ticket_events t ON t.id = s.event_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        event_at_year,
        ticket_priority,
        ticket_channel,
        event_id,
        event_event_at,
        CONCAT(ticket_channel, '#', CAST(ticket_id AS STRING)) AS ticket_channel_ref,
        COALESCE(ticket_priority, 'UNKNOWN') AS ticket_priority_or_unknown
    FROM joined
)
SELECT
    id,
    event_at_year,
    event_id,
    event_event_at,
    ticket_channel_ref,
    ticket_priority_or_unknown,
    COALESCE(event_at_year, 0) AS event_at_year_filled,
    CASE WHEN ticket_channel IN ('chat', 'phone') THEN 'group_a' WHEN ticket_channel = 'email' THEN 'group_b' ELSE 'other' END AS ticket_channel_group
FROM enriched
ORDER BY event_at_year_filled

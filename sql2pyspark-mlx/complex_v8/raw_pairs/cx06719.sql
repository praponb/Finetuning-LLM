WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM joined_ranges
    WHERE agent_id <= 9
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(priority_group), ' ', '_')) AS priority_group_slug,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM derived
    WHERE ticket_id >= 53
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM annotated s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
        )
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    agent_id,
    hours_since_open,
    priority_group_slug,
    event_at + INTERVAL 2 HOURS AS event_at_plus_2h
FROM filtered
ORDER BY event_at_plus_2h

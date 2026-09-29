WITH matched AS (
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
annotated AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        CASE WHEN event_type IN ('reopened', 'assigned') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        MONTH(event_at) AS event_at_month,
        ROUND(hours_since_open * 1.5, 2) AS hours_since_open_adj
    FROM matched
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_group,
        event_at_month,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type_group), ' ', '_')) AS event_type_group_slug,
        CASE WHEN event_at_month >= 9 THEN 'A' WHEN event_at_month >= 6 THEN 'B' ELSE 'C' END AS event_at_month_tier
    FROM annotated
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_group,
    event_at_month,
    event_at_label,
    event_at_month_tier
FROM enriched
ORDER BY event_type_group DESC, event_at_month DESC

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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
deduped AS (
    -- Keep the latest row per ticket_id, agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, agent_id ORDER BY hours_since_open DESC, id DESC) AS recency_rank
        FROM matched
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        CONCAT_WS(' / ', event_type, priority) AS event_type_priority_label
    FROM deduped
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        event_type_priority_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        ROUND(hours_since_open * 1.5, 2) AS hours_since_open_adj,
        HOUR(event_at) AS event_at_hour
    FROM annotated
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    event_type_priority_label,
    days_since_event_at,
    hours_since_open_adj,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
    days_since_event_at - event_at_hour AS days_since_event_at_minus_event_at_hour
FROM tagged

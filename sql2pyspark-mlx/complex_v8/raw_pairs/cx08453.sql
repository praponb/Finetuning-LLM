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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        priority,
        hours_since_open,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref
    FROM joined_ranges
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM annotated s
),
latest AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_type,
        priority,
        hours_since_open,
        priority_ref,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY first_event_at DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    event_type,
    priority,
    hours_since_open,
    priority_ref,
    first_event_at,
    LOWER(REGEXP_REPLACE(TRIM(priority_ref), ' ', '_')) AS priority_ref_slug
FROM latest
ORDER BY first_event_at, priority_ref_slug DESC

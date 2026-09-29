WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 15
      AND ARRAY_CONTAINS(tags, 'bug')
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        closed_at,
        csat,
        tags,
        channel_or_unknown,
        least_csat_requester_id,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM base_tickets
    WHERE agent_id >= 8
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.closed_at,
        s.csat,
        s.channel_or_unknown,
        s.priority_or_unknown,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM labeled s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.actor = 'agent'
),
deduped AS (
    -- Keep the earliest row per priority_or_unknown, event_actor
    SELECT
        id,
        closed_at,
        csat,
        channel_or_unknown,
        priority_or_unknown,
        event_id,
        event_at,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority_or_unknown, event_actor ORDER BY csat, id, event_id) AS recency_rank
        FROM detail_rows_events
    ) ranked
    WHERE recency_rank = 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM deduped s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_details_event AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.closed_at,
        s.csat,
        s.channel_or_unknown,
        s.event_id,
        s.event_at,
        s.event_actor,
        t.event_type,
        ti.requester_id AS ticket_requester_id
    FROM with_stats s
    JOIN ticket_events t ON t.id = s.event_id
    JOIN tickets ti ON ti.id = t.ticket_id
    WHERE t.event_type IS NOT NULL
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    channel_or_unknown,
    event_at,
    event_actor,
    event_type,
    ticket_requester_id
FROM with_details_event

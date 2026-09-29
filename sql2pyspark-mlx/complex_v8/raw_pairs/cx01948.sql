WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-19 11:00:00'
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_parent_2 AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.ticket_closed_at,
        t.channel AS ticket_channel,
        t.tags AS ticket_tags
    FROM with_parent s
    JOIN tickets t ON t.id = s.ticket_id AND t.csat <= 4
    WHERE SIZE(t.tags) >= 1
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IN ('reopened', 'assigned')) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count
    FROM with_parent_2 s
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ticket_closed_at + INTERVAL 15 MINUTES AS ticket_closed_at_plus_15m,
        LEAST(event_event_type_count, event_count) AS least_event_event_type_count_event_count
    FROM with_stats
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM enriched s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
),
with_parent_3 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.ticket_closed_at,
        s.ticket_tags,
        s.event_count,
        s.ticket_closed_at_plus_15m,
        s.least_event_event_type_count_event_count,
        s.first_event_at,
        t.tags AS ticket_tags_2,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id
    FROM profiled s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    event_type,
    ticket_closed_at,
    ticket_tags,
    event_count,
    least_event_event_type_count_event_count,
    ticket_tags_2,
    ticket_csat,
    ticket_requester_id,
    DATE_TRUNC('HOUR', ticket_closed_at) AS ticket_closed_at_hour
FROM with_parent_3
ORDER BY ticket_closed_at_hour DESC

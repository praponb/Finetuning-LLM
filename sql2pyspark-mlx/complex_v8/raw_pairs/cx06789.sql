WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_event_at,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.priority = 'P1'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_csat,
        ticket_priority,
        CONCAT(ticket_priority, '#', CAST(ticket_id AS STRING)) AS ticket_priority_ref
    FROM with_details
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FILTER (WHERE t.actor IN ('system', 'customer')) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count
    FROM scored s
),
ticket_id_stats AS (
    -- Median event_actor_count per ticket_id
    SELECT ticket_id, PERCENTILE_APPROX(event_actor_count, 0.5) AS median_event_actor_count
    FROM with_stats
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_actor_count,
        ROUND(s.event_actor_count - g.median_event_actor_count, 2) AS event_actor_count_minus_median
    FROM with_stats s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_actor_count >= g.median_event_actor_count
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(ticket_priority, 'UNKNOWN') AS ticket_priority_or_unknown
    FROM above_average
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.ticket_csat,
        s.ticket_priority,
        s.ticket_priority_ref,
        s.first_event_at,
        s.median_event_actor_count,
        s.event_actor_count_minus_median,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat_2
    FROM labeled s
    JOIN tickets t ON t.id = s.ticket_id AND t.csat <= 4
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, ticket_csat, ticket_csat_2
    FROM with_details_ticket
) src
UNPIVOT (
    metric_value FOR measure IN (ticket_csat, ticket_csat_2)
) unp

WITH filtered_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 7 AND 12
      AND ticket_id >= 36
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM filtered_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        event_type,
        YEAR(event_at) AS event_at_year,
        SUM(CASE WHEN actor IN ('system', 'customer') THEN 1 ELSE 0 END) AS n_actor_in_system_customer,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE event_event_type <> 'created') AS n_event_event_type_created,
        MIN(event_at) AS first_event_at,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_children_events
    GROUP BY 1, 2, 3
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.event_type,
        s.event_at_year,
        s.n_actor_in_system_customer,
        s.last_event_at,
        s.first_event_at,
        s.distinct_ids,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id
    FROM per_group s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT distinct_ids, event_type, event_at_year
    FROM combined_details_ticket
) src
PIVOT (
    SUM(event_at_year)
    FOR event_type IN ('escalated', 'assigned', 'created')
)
ORDER BY distinct_ids

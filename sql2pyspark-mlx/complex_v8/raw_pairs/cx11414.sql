WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 09:00:00'
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at_day,
        s.days_since_event_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM valid_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_actor,
        s.event_event_at,
        t.id AS event_id_2,
        t.event_type,
        t.event_at
    FROM with_children_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
joined_event AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_actor,
        s.event_id_2,
        s.event_type,
        s.event_at,
        t.actor AS event_actor_2,
        t.ticket_id AS event_ticket_id,
        t.event_type AS event_event_type
    FROM with_children s
    LEFT JOIN ticket_events t ON t.id = s.event_id_2
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_actor,
        s.event_id_2,
        s.event_type,
        s.event_at,
        s.event_actor_2,
        s.event_ticket_id,
        s.event_event_type,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM joined_event s
    JOIN tickets t ON t.id = s.event_ticket_id
    WHERE t.channel IN ('chat', 'web')
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_parent_ticket
    WHERE ticket_requester_id > (SELECT MIN(ticket_requester_id) FROM with_parent_ticket)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_id_2,
        s.event_at,
        s.event_actor_2,
        s.event_ticket_id,
        s.event_event_type,
        s.ticket_channel,
        t.csat AS ticket_csat_2
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_id,
    event_id_2,
    event_at,
    event_ticket_id,
    event_event_type,
    ticket_channel
FROM with_details
ORDER BY event_id DESC

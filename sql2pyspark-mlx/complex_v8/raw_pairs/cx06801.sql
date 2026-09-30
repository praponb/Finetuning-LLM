WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        t.agent_id AS ticket_agent_id
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id AND MONTH(t.closed_at) IN (2, 3)
    WHERE t.agent_id IS NOT NULL
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_agent_id,
        t.id AS ticket_id_2,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM enriched s
    JOIN tickets t ON t.agent_id = s.ticket_agent_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_agent_id,
        s.ticket_id_2,
        s.ticket_tags,
        s.ticket_channel,
        s.ticket_requester_id,
        t.id AS event_id,
        t.event_type,
        t.event_at AS event_event_at
    FROM exploded_detail_tickets s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY event_id), 2) AS event_id_avg_ticket_requester_id,
        SUM(ticket_requester_id) OVER w_event_id AS event_id_sum_ticket_requester_id,
        ROW_NUMBER() OVER w_ticket_agent_id_by_ticket_re AS ticket_requester_id_seq
    FROM exploded_detail
    WINDOW w_event_id AS (PARTITION BY event_id), w_ticket_agent_id_by_ticket_re AS (PARTITION BY ticket_agent_id ORDER BY ticket_requester_id, id, ticket_id_2, event_id)
),
unique_rows AS (
    -- Keep the latest row per ticket_agent_id, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_agent_id,
        ticket_id_2,
        ticket_tags,
        ticket_channel,
        ticket_requester_id,
        event_id,
        event_type,
        event_event_at,
        event_id_avg_ticket_requester_id,
        event_id_sum_ticket_requester_id,
        ticket_requester_id_seq
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_agent_id, ticket_id ORDER BY event_at DESC, id DESC, ticket_id_2 DESC, event_id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_id, event_type, ticket_requester_id_seq
    FROM unique_rows
) src
PIVOT (
    SUM(ticket_requester_id_seq) AS total, COUNT(ticket_requester_id_seq) AS n
    FOR event_type IN ('escalated', 'assigned', 'replied', 'reopened')
)

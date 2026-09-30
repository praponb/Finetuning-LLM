WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label
    FROM tickets
    WHERE (SIZE(tags) >= 2 OR requester_id < 17)
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.closed_at_plus_7d,
        s.days_since_closed_at,
        s.opened_at_label,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        s.days_since_closed_at,
        s.agent_hired_on,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM combined_details s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type = 'replied'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN event_type NOT IN ('reopened', 'replied') THEN 1 END) AS event_event_type_count,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        t.first_event_at
    FROM detail_rows s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
combined_details_event AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        s.days_since_closed_at,
        s.agent_hired_on,
        s.event_id,
        s.event_at,
        s.event_actor,
        s.event_type,
        s.event_event_type_count,
        s.first_event_at,
        t.event_at AS event_event_at,
        t.actor AS event_actor_2,
        t.event_type AS event_event_type
    FROM summarized_rows s
    LEFT JOIN ticket_events t ON t.id = s.event_id
)
SELECT DISTINCT event_actor_2, requester_id, agent_hired_on
FROM combined_details_event
ORDER BY agent_hired_on DESC NULLS FIRST, requester_id DESC

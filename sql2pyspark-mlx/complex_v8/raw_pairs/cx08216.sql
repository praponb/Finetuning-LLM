WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 12:00:00'
      AND actor IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN actor IS NOT NULL THEN 1 END) AS event_actor_count,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.last_event_at
    FROM cleaned_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_count,
        s.event_actor_count,
        s.last_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_count,
        s.event_actor_count,
        s.last_event_at,
        s.event_id,
        s.event_event_type,
        s.event_event_at,
        s.event_actor,
        t.opened_at AS ticket_opened_at,
        su.agent_name,
        su.team AS agent_team
    FROM line_level s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE HOUR(t.opened_at) BETWEEN 8 AND 10
)
-- Subtotals with CUBE
SELECT
    agent_team,
    event_actor_count,
    COUNT(*) AS row_count,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'J%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_agent_name_like_j
FROM with_details_ticket
GROUP BY CUBE(agent_team, event_actor_count)

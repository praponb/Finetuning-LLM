WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor <> 'system'
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
),
line_level_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at_label,
        s.days_since_event_at,
        s.ticket_csat,
        s.ticket_agent_id,
        s.ticket_channel,
        t.id AS ticket_id_2,
        t.csat AS ticket_csat_2,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority
    FROM combined_details_ticket s
    LEFT JOIN tickets t ON t.agent_id = s.ticket_agent_id AND t.csat >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ticket_csat_2 / NULLIF(SUM(ticket_csat_2) OVER w_ticket_channel, 0), 4) AS ticket_csat_2_share_of_ticket_channel,
        LAG(days_since_event_at) OVER (PARTITION BY ticket_channel ORDER BY days_since_event_at DESC, id, ticket_id_2) AS prev_days_since_event_at,
        ROUND(ticket_csat_2 - AVG(ticket_csat_2) OVER (PARTITION BY ticket_channel), 2) AS ticket_csat_2_vs_ticket_channel_avg,
        MAX(ticket_csat) OVER (PARTITION BY ticket_channel) AS ticket_channel_max_ticket_csat,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY ticket_channel ORDER BY days_since_event_at DESC, id, ticket_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at
    FROM line_level_tickets
    WINDOW w_ticket_channel AS (PARTITION BY ticket_channel)
)
SELECT
    ticket_csat,
    ticket_channel,
    ticket_priority,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM sequenced
GROUP BY GROUPING SETS ((ticket_csat, ticket_channel, ticket_priority), (ticket_csat), (ticket_channel), ())
ORDER BY ticket_csat NULLS LAST, ticket_channel NULLS LAST, ticket_priority NULLS LAST

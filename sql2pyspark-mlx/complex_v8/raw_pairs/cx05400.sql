WITH in_window AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY ticket_id, event_type)) AS distinct_agent_id_in_ticket_id_event_type,
        LAG(event_at) OVER (ORDER BY hours_since_open, id) AS prev_event_at,
        SUM(hours_since_open) OVER (ORDER BY hours_since_open, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hours_since_open_sum_7,
        SUM(hours_since_open) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS hours_since_open_sum_1_prior,
        ROUND(hours_since_open - AVG(hours_since_open) OVER w_ticket_id_event_type, 2) AS hours_since_open_vs_ticket_id_event_type_avg,
        SIZE(COLLECT_SET(priority) OVER w_ticket_id_event_type) AS distinct_priority_in_ticket_id_event_type
    FROM in_window
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type)
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IN ('T2', 'T1')
              AND LENGTH(TRIM(su.agent_name)) >= 5
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.prev_event_at,
        s.hours_since_open_sum_7,
        s.hours_since_open_sum_1_prior,
        s.hours_since_open_vs_ticket_id_event_type_avg,
        s.distinct_priority_in_ticket_id_event_type,
        su.agent_name,
        t.channel AS ticket_channel
    FROM without_matches s
    LEFT JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    hours_since_open,
    prev_event_at,
    hours_since_open_sum_7,
    hours_since_open_vs_ticket_id_event_type_avg,
    ticket_channel,
    LEAST(hours_since_open, hours_since_open_sum_7) AS least_hours_since_open_hours_since_open_sum_7,
    HOUR(prev_event_at) AS prev_event_at_hour
FROM joined
ORDER BY ticket_channel DESC NULLS FIRST

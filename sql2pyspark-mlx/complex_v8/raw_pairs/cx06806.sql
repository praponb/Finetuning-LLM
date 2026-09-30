WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 09:00:00'
      AND ticket_id <= 58
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at, 3) OVER w_ticket_id_by_event_at AS prev3_event_at,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at, 3) AS event_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM prepared
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id)
),
ticket_id_stats AS (
    -- Median minutes_since_prev_event_at per ticket_id
    SELECT ticket_id, PERCENTILE_APPROX(minutes_since_prev_event_at, 0.5) AS median_minutes_since_prev_event_at
    FROM with_windows
    GROUP BY ticket_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_minutes_since_prev_event_at,
        ROUND(s.minutes_since_prev_event_at - g.median_minutes_since_prev_event_at, 2) AS minutes_since_prev_event_at_minus_median
    FROM with_windows s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.minutes_since_prev_event_at >= g.median_minutes_since_prev_event_at
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.second_event_at,
        s.prev3_event_at,
        s.event_at_percent_rank,
        s.minutes_since_prev_event_at,
        s.prev_event_at,
        s.median_minutes_since_prev_event_at,
        s.minutes_since_prev_event_at_minus_median,
        t.requester_id AS ticket_requester_id,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority
    FROM vs_group s
    JOIN tickets t ON t.id = s.ticket_id AND t.closed_at >= TIMESTAMP '2025-02-02 03:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM enriched_ticket s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    second_event_at,
    minutes_since_prev_event_at,
    prev_event_at,
    median_minutes_since_prev_event_at,
    minutes_since_prev_event_at_minus_median,
    ticket_requester_id,
    ticket_agent_id,
    ticket_priority,
    agent_name,
    agent_hired_on,
    agent_team,
    ABS(event_at_percent_rank - 0.7) AS event_at_percent_rank_distance
FROM joined
WHERE event_at >= TIMESTAMP '2025-01-31 08:00:00'
ORDER BY agent_hired_on

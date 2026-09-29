WITH matched AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(event_at, 3) OVER (ORDER BY hours_since_open, id) AS prev3_event_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY YEAR(event_at))) AS distinct_event_type_in_event_at_year,
        LAG(hours_since_open, 2) OVER (ORDER BY hours_since_open, id) AS prev2_hours_since_open,
        COUNT(*) OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM matched
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(requester_id) AS min_ticket_requester_id,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.min_ticket_requester_id,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM sequenced s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(min_ticket_requester_id) OVER w_ticket_count, 2) AS ticket_count_avg_min_ticket_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_count ORDER BY ticket_count DESC), 3) AS ticket_count_cume_dist,
        COUNT(*) OVER (PARTITION BY ticket_count) AS rows_in_ticket_count,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_count)) AS distinct_event_type_in_ticket_count,
        prev2_hours_since_open - LAG(prev2_hours_since_open) OVER (PARTITION BY ticket_count ORDER BY ticket_count DESC, id) AS prev2_hours_since_open_change
    FROM with_stats
    WINDOW w_ticket_count AS (PARTITION BY ticket_count)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    prev3_event_at,
    distinct_event_type_in_event_at_year,
    prev2_hours_since_open,
    running_count,
    min_ticket_requester_id,
    ticket_count,
    ticket_count_avg_min_ticket_requester_id,
    ticket_count_cume_dist,
    rows_in_ticket_count,
    distinct_event_type_in_ticket_count,
    prev2_hours_since_open_change
FROM with_windows
ORDER BY running_count DESC, ticket_count_cume_dist DESC NULLS FIRST

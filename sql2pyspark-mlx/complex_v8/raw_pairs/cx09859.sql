WITH covered AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN hours_since_open >= 43.2 THEN 'large' WHEN hours_since_open >= 14.4 THEN 'medium' ELSE 'small' END AS hours_since_open_tier,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM covered
    WHERE hours_since_open > 28.8
),
exploded_detail AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open_tier,
        s.event_type_or_unknown,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM scored s
    JOIN tickets t ON t.agent_id = s.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(ticket_closed_at, 3) OVER (ORDER BY ticket_closed_at DESC, id, ticket_id_2) AS next3_ticket_closed_at,
        LEAD(ticket_closed_at) OVER (ORDER BY ticket_closed_at DESC, id, ticket_id_2) AS next_ticket_closed_at,
        MIN(ticket_csat) OVER (PARTITION BY ticket_channel) AS ticket_channel_min_ticket_csat,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER w_all_by_ticket_closed_at)) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        MAX(ticket_csat) OVER w_ticket_channel AS ticket_channel_max_ticket_csat,
        RANK() OVER (ORDER BY ticket_closed_at DESC, id, ticket_id_2) AS ticket_closed_at_rank
    FROM exploded_detail
    WINDOW w_all_by_ticket_closed_at AS (ORDER BY ticket_closed_at DESC, id, ticket_id_2), w_ticket_channel AS (PARTITION BY ticket_channel)
)
-- Subtotals with ROLLUP
SELECT
    event_type,
    hours_since_open_tier,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(ticket_csat), 2) AS stddev_ticket_csat,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
    MAX(ticket_channel_max_ticket_csat) AS max_ticket_channel_max_ticket_csat
FROM with_windows
GROUP BY ROLLUP(event_type, hours_since_open_tier)
ORDER BY event_type NULLS LAST, hours_since_open_tier NULLS LAST

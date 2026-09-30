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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
per_group AS (
    -- Group-level totals
    SELECT
        priority,
        agent_id,
        TO_DATE(event_at) AS event_at_day,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open,
        SUM(CASE WHEN priority <> 'P1' THEN hours_since_open ELSE 0 END) AS hours_since_open_priority_p1,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM matched
    GROUP BY priority, agent_id, TO_DATE(event_at)
),
scored AS (
    -- Add calculated columns
    SELECT
        priority,
        agent_id,
        event_at_day,
        distinct_ids,
        total_hours_since_open,
        priority_list,
        LEAST(total_hours_since_open, distinct_ids) AS least_total_hours_since_open_distinct_ids
    FROM per_group
    WHERE SIZE(priority_list) >= 2
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id,
        (SELECT COUNT(CASE WHEN t.channel NOT IN ('chat', 'email') THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM scored s
)
-- Final projection
SELECT
    priority,
    agent_id,
    event_at_day,
    distinct_ids,
    total_hours_since_open,
    priority_list,
    max_ticket_requester_id,
    ticket_channel_count,
    ticket_count,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
    LENGTH(TRIM(priority)) AS priority_len
FROM with_counts
ORDER BY agent_id DESC NULLS FIRST

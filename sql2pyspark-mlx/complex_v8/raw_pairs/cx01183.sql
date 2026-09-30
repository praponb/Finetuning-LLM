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
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.priority IN ('P4', 'P1', 'P2') THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM matched s
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND s.ticket_count BETWEEN 2 AND 7
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        hours_since_open,
        ticket_priority_count,
        ticket_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY ticket_priority_count), 3) AS ticket_priority_count_percent_rank,
        NTH_VALUE(ticket_priority_count, 2) OVER (PARTITION BY agent_id ORDER BY ticket_priority_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_priority_count,
        ROUND(ticket_count / NULLIF(SUM(ticket_count) OVER w_priority, 0), 4) AS ticket_count_share_of_priority,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority
    FROM filtered
    WINDOW w_priority AS (PARTITION BY priority)
)
-- Final projection
SELECT
    id,
    ticket_id,
    hours_since_open,
    ticket_priority_count,
    ticket_count,
    ticket_priority_count_percent_rank,
    second_ticket_priority_count,
    ticket_count_share_of_priority,
    rows_in_priority
FROM windowed
WHERE ticket_priority_count_percent_rank < 0.7
ORDER BY rows_in_priority DESC

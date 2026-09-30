WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        csat + requester_id AS csat_plus_requester_id,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h
    FROM tickets
    WHERE priority <> 'P2'
      AND opened_at < TIMESTAMP '2025-02-24 14:00:00'
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at_2
    FROM scoped s
)
SELECT
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING(channel) AS is_total_channel,
    MIN(csat_plus_requester_id) AS min_csat_plus_requester_id,
    ROUND(AVG(csat_plus_requester_id), 2) AS avg_csat_plus_requester_id
FROM profiled
GROUP BY CUBE(channel, priority)
ORDER BY channel NULLS LAST, priority NULLS LAST

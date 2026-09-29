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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
deduped AS (
    -- Keep the earliest row per ticket_id, agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, agent_id ORDER BY hours_since_open, id) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    agent_id,
    COALESCE(priority, 'ALL') AS priority,
    MIN(hours_since_open) AS min_hours_since_open,
    SUM(hours_since_open) FILTER (WHERE priority IN ('P4', 'P1', 'P2')) AS hours_since_open_priority_in_p4_p1_p2,
    ROUND(100 * COUNT(CASE WHEN event_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_type_is_not_null
FROM deduped
GROUP BY GROUPING SETS ((event_type, agent_id, priority), (event_type))

WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor = 'system'
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.days_since_event_at,
        s.prev_event_at,
        s.new_session_flag,
        t.priority AS ticket_priority,
        su.team AS agent_team
    FROM sessionized s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
event_type_stats AS (
    -- Avg days_since_event_at per event_type
    SELECT event_type, AVG(days_since_event_at) AS avg_days_since_event_at
    FROM enriched
    GROUP BY event_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at,
        ROUND(s.days_since_event_at - g.avg_days_since_event_at, 2) AS days_since_event_at_minus_avg
    FROM enriched s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.days_since_event_at > g.avg_days_since_event_at
)
-- Final projection
SELECT
    id,
    event_type,
    days_since_event_at,
    prev_event_at,
    new_session_flag,
    ticket_priority,
    agent_team,
    avg_days_since_event_at,
    days_since_event_at_minus_avg,
    DATEDIFF(DATE '2025-12-31', prev_event_at) AS days_since_prev_event_at,
    QUARTER(prev_event_at) AS prev_event_at_quarter
FROM compared
ORDER BY agent_team DESC, id
LIMIT 5

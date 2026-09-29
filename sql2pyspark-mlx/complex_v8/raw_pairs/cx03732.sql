WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 16
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
with_parent AS (
    -- Join tickets
    SELECT
        s.*,
        t.channel AS ticket_channel,
        su.agent_name,
        su.team AS agent_team
    FROM session_summary s
    JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.closed_at) BETWEEN 10 AND 15
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM with_parent s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_count, ticket_id, agent_team, duration_min
    FROM profiled
) src
PIVOT (
    MAX(duration_min) AS max_v, SUM(duration_min) AS sum_v
    FOR agent_team IN ('VIP', 'Billing')
)
ORDER BY event_count, ticket_id

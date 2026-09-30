WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(duration_min) OVER (ORDER BY n_events, agent_id, burst_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_min_sum_2,
        COUNT(*) OVER w_session_start_month_agent_id AS rows_in_session_start_month_agent_id,
        MAX(duration_min) OVER w_session_start_month_agent_id AS session_start_month_agent_id_max_duration_min,
        ROUND(CUME_DIST() OVER (ORDER BY n_events, agent_id, burst_id), 3) AS n_events_cume_dist,
        NTILE(3) OVER (ORDER BY n_events, agent_id, burst_id) AS n_events_tercile
    FROM per_session
    WINDOW w_session_start_month_agent_id AS (PARTITION BY DATE_TRUNC('MONTH', session_start), agent_id)
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    session_end,
    n_events,
    duration_min,
    duration_min_sum_2,
    rows_in_session_start_month_agent_id,
    n_events_cume_dist,
    n_events_tercile
FROM sequenced
ORDER BY n_events_cume_dist DESC

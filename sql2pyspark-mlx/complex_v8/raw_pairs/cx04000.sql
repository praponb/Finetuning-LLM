WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        agent_id,
        session_id,
        session_start,
        session_end,
        events,
        session_csat,
        duration_min,
        DENSE_RANK() OVER (ORDER BY session_end, agent_id, session_id) AS session_end_dense_rank,
        COUNT(*) OVER (PARTITION BY events) AS rows_in_events,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY events)) AS distinct_agent_id_in_events,
        duration_min - LAG(duration_min) OVER (ORDER BY session_end, agent_id, session_id) AS duration_min_change
    FROM sessions
)
SELECT
    session_id,
    events,
    PERCENTILE_APPROX(session_csat, 0.5) AS median_session_csat,
    SUM(distinct_agent_id_in_events) AS total_distinct_agent_id_in_events,
    MIN(session_start) AS first_session_start
FROM ranked
GROUP BY GROUPING SETS ((session_id, events), (session_id), (events), ())

WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.agent_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.duration_min,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM session_summary s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(duration_min) IGNORE NULLS OVER (PARTITION BY agent_team, agent_id ORDER BY session_start, agent_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_min,
        MAX(session_no) OVER (PARTITION BY agent_team, agent_id ORDER BY session_start, agent_id, session_no ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS session_no_max_7,
        ROUND(AVG(session_no) OVER (PARTITION BY agent_team, agent_id ORDER BY session_start, agent_id, session_no ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS session_no_avg_2,
        COUNT(*) OVER (PARTITION BY TO_DATE(session_end)) AS rows_in_session_end_to_date,
        SIZE(COLLECT_SET(agent_team) OVER w_session_end_to_date) AS distinct_agent_team_in_session_end_to_date
    FROM with_parent_agent
    WINDOW w_session_end_to_date AS (PARTITION BY TO_DATE(session_end))
)
-- Final projection
SELECT
    agent_id,
    session_start,
    session_end,
    duration_min,
    agent_team,
    agent_hired_on,
    agent_name,
    session_no_max_7,
    session_no_avg_2,
    rows_in_session_end_to_date,
    distinct_agent_team_in_session_end_to_date,
    LEAST(duration_min, session_no_avg_2) AS least_duration_min_session_no_avg_2,
    UPPER(TRIM(agent_name)) AS agent_name_clean
FROM metrics
ORDER BY agent_name_clean DESC NULLS FIRST

WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE agent_id <= 6
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
per_group AS (
    -- Summarise per group
    SELECT
        agent_id,
        burst_id,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        MAX(session_end) AS last_session_end,
        SUM(duration_min) AS total_duration_min,
        MAX(duration_min) AS max_duration_min,
        PERCENTILE_APPROX(duration_min, 0.5) AS median_duration_min,
        SUM(burst_id) AS total_burst_id
    FROM sessions
    WHERE n_events BETWEEN 1 AND 4
    GROUP BY agent_id, burst_id, DATE_TRUNC('MONTH', session_start)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(max_duration_min) OVER (PARTITION BY burst_id ORDER BY total_duration_min ASC, agent_id, burst_id, session_start_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_duration_min,
        ROUND((UNIX_TIMESTAMP(session_start_month) - UNIX_TIMESTAMP(LAG(session_start_month) OVER (ORDER BY total_duration_min ASC, agent_id, burst_id, session_start_month))) / 60, 1) AS minutes_since_prev_session_start_month,
        SUM(max_duration_min) OVER w_agent_id_2 AS agent_id_sum_max_duration_min
    FROM per_group
    WINDOW w_agent_id_2 AS (PARTITION BY agent_id)
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    session_start_month,
    total_duration_min,
    max_duration_min,
    total_burst_id,
    last_max_duration_min,
    minutes_since_prev_session_start_month,
    agent_id_sum_max_duration_min,
    ROUND(max_duration_min * 1.1, 2) AS max_duration_min_adj
FROM sequenced
ORDER BY total_burst_id

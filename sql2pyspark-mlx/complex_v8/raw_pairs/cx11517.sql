WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE csat BETWEEN 1 AND 4
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(duration_min - AVG(duration_min) OVER (PARTITION BY YEAR(session_start)), 2) AS duration_min_vs_session_start_year_avg,
        LAST_VALUE(session_start) IGNORE NULLS OVER (PARTITION BY burst_id, event_count ORDER BY event_count ASC NULLS LAST, agent_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_start,
        NTH_VALUE(event_count, 2) OVER (PARTITION BY burst_id, agent_id ORDER BY event_count, agent_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_count,
        DENSE_RANK() OVER (PARTITION BY burst_id, agent_id ORDER BY event_count, agent_id, burst_id) AS event_count_dense_rank,
        DENSE_RANK() OVER (PARTITION BY burst_id, event_count ORDER BY event_count ASC NULLS LAST) AS event_count_dense_rank_2,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY YEAR(session_start))) AS distinct_agent_id_in_session_start_year
    FROM sessions
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM sequenced s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    agent_id,
    session_end,
    event_count,
    session_csat,
    duration_min_vs_session_start_year_avg,
    last_known_session_start,
    event_count_dense_rank_2,
    distinct_agent_id_in_session_start_year,
    session_start + INTERVAL 1 DAYS AS session_start_plus_1d,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 3600, 1) AS hours_session_end_to_session_start
FROM profiled
ORDER BY session_start_plus_1d DESC, event_count_dense_rank_2

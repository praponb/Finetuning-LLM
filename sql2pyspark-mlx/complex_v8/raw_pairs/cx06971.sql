WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 12:00:00'
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.agent_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.event_count,
        s.duration_min,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM sessions s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        agent_id,
        session_id,
        session_start,
        event_count,
        agent_tier,
        agent_hired_on,
        agent_team,
        ROW_NUMBER() OVER w_agent_id_by_duration_min AS rn,
        SUM(duration_min) OVER (PARTITION BY session_id) AS session_id_sum_duration_min,
        SUM(event_count) OVER (PARTITION BY event_count ORDER BY event_count NULLS LAST, agent_id, session_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_count_sum_6_prior,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY duration_min DESC, agent_id, session_id) AS rn_2
    FROM with_parent_agent
    WINDOW w_agent_id_by_duration_min AS (PARTITION BY agent_id ORDER BY duration_min, agent_id, session_id)
)
SELECT
    agent_id,
    session_start,
    event_count,
    agent_tier,
    agent_hired_on,
    agent_team,
    rn,
    session_id_sum_duration_min,
    event_count_sum_6_prior,
    rn_2,
    QUARTER(session_start) AS session_start_quarter
FROM with_trends
ORDER BY session_start_quarter DESC

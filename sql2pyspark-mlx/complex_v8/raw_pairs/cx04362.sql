WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
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
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team
    FROM sessions s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team <> 'Billing'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_min) OVER (PARTITION BY agent_team) AS agent_team_sum_duration_min,
        ROUND(event_count / NULLIF(SUM(event_count) OVER (PARTITION BY agent_team), 0), 4) AS event_count_share_of_agent_team
    FROM combined_details_agent
)
SELECT *
FROM (
    SELECT agent_id, session_end, agent_team, burst_id
    FROM ranked
) src
PIVOT (
    AVG(burst_id) AS avg_v, SUM(burst_id) AS sum_v
    FOR agent_team IN ('Tech', 'VIP', 'Billing', 'Onboarding')
)

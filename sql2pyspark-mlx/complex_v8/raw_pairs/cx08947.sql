WITH valid_tickets AS (
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
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE agent_id >= 9
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.agent_id,
        s.session_no,
        s.events,
        s.duration_min,
        su.agent_name,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM sessions s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'J%'
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.agent_id,
        s.session_no,
        s.agent_tier,
        su.tier AS agent_tier_2,
        su.agent_name AS agent_agent_name,
        su.team AS agent_team_2
    FROM joined s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    session_no,
    agent_tier,
    agent_tier_2,
    agent_agent_name,
    COALESCE(session_no, 0) AS session_no_filled
FROM enriched_agent
ORDER BY agent_tier, agent_tier_2 DESC NULLS FIRST

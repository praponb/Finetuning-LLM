WITH prepared AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE csat <= 4
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.agent_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.event_count,
        s.session_csat,
        s.duration_min,
        su.tier AS agent_tier,
        su.agent_name
    FROM session_summary s
    JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(agent_tier), 1, 3) AS agent_tier_prefix,
        CONCAT_WS(' / ', agent_name, agent_tier) AS agent_name_agent_tier_label,
        ROUND(session_no / NULLIF(session_csat, 0), 3) AS session_no_per_session_csat
    FROM with_parent_agent
)
-- Subtotals with ROLLUP
SELECT
    event_count,
    agent_id,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    ROUND(SUM(duration_min) / NULLIF(SUM(session_csat), 0), 3) AS duration_min_to_session_csat_ratio,
    ROUND(STDDEV(event_count), 2) AS stddev_event_count
FROM enriched
GROUP BY ROLLUP(event_count, agent_id, agent_tier)
ORDER BY event_count NULLS LAST, agent_id NULLS LAST, agent_tier NULLS LAST

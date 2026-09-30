WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        prev_closed_at,
        new_session_flag,
        session_no
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id DESC) AS recency_rank
        FROM sessionized
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    csat,
    tags,
    new_session_flag,
    session_no,
    new_session_flag + session_no AS new_session_flag_plus_session_no,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM latest_per_key
ORDER BY id DESC, agent_id

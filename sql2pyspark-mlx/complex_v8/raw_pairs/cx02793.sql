WITH scoped_tickets AS (
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
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
unique_rows AS (
    -- Keep the latest row per agent_id, channel
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
        prev_opened_at,
        new_session_flag,
        session_no
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY new_session_flag DESC, id DESC) AS recency_rank
        FROM sessionized
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY csat, DATE_TRUNC('MONTH', prev_opened_at) ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        MAX(session_no) OVER (PARTITION BY session_no, channel ORDER BY prev_opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_session_no_so_far,
        MAX(new_session_flag) OVER (PARTITION BY agent_id) AS agent_id_max_new_session_flag,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id
    FROM unique_rows
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    csat,
    tags,
    prev_opened_at,
    new_session_flag,
    first_known_closed_at,
    max_session_no_so_far,
    agent_id_max_new_session_flag,
    requester_id_dense_rank,
    CONCAT_WS(' / ', priority, channel) AS priority_channel_label,
    CONCAT(priority, '#', CAST(requester_id_dense_rank AS STRING)) AS priority_ref
FROM with_trends
ORDER BY agent_id DESC, id
LIMIT 20

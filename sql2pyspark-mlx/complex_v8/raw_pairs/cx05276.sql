WITH base AS (
    -- Filter tickets
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
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id_ties, 3) AS requester_id_percent_rank,
        COUNT(*) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY priority, channel), 0), 4) AS requester_id_share_of_priority_channel,
        MAX(session_id) OVER w_priority_channel AS priority_channel_max_session_id,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC, id) AS seq_no
    FROM sessionized
    WINDOW w_all_by_requester_id_ties AS (ORDER BY requester_id DESC), w_priority_channel AS (PARTITION BY priority, channel)
),
deduped AS (
    -- Keep the latest row per channel
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
        channel_slug,
        prev_opened_at,
        is_new_session,
        session_id,
        requester_id_percent_rank,
        running_count,
        requester_id_share_of_priority_channel,
        priority_channel_max_session_id,
        seq_no
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_count DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
SELECT
    priority,
    is_new_session,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT id) AS distinct_ids
FROM deduped
GROUP BY GROUPING SETS ((priority, is_new_session), (priority), (is_new_session), ())
ORDER BY priority NULLS LAST, is_new_session NULLS LAST

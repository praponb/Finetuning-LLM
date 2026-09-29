WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        priority_ref,
        is_new_session,
        burst_id + is_new_session AS burst_id_plus_is_new_session,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM sessionized
    WHERE NOT ARRAY_CONTAINS(tags, 'login')
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    tags,
    priority_ref,
    burst_id_plus_is_new_session,
    channel_or_unknown,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
    DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter
FROM enriched
WHERE burst_id_plus_is_new_session < 5
ORDER BY agent_id NULLS LAST, opened_at, id
LIMIT 10

WITH recent_tickets AS (
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
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        channel,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, burst_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(session_end) AS session_end_year,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_label,
        ROUND(burst_id / NULLIF(events, 0), 3) AS burst_id_per_events
    FROM session_summary
)
-- Final projection
SELECT
    channel,
    burst_id,
    session_start,
    session_end,
    events,
    duration_min,
    session_end_year,
    session_end_label,
    duration_min - burst_id AS duration_min_minus_burst_id
FROM enriched
ORDER BY duration_min_minus_burst_id, duration_min DESC, channel, burst_id
LIMIT 5

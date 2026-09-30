WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE requester_id >= 5
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_no
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE event_count > (SELECT AVG(event_count) FROM per_session)
),
scored AS (
    -- Derive helper columns
    SELECT
        channel,
        session_no,
        session_start,
        session_end,
        event_count,
        duration_min,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        CASE WHEN duration_min >= 144.0 THEN 'HIGH' WHEN duration_min >= 108.0 THEN 'MEDIUM' ELSE 'LOW' END AS duration_min_tier
    FROM screened
    WHERE session_no > 2
)
SELECT DISTINCT duration_min
FROM scored
WHERE event_count >= 2
ORDER BY duration_min NULLS LAST

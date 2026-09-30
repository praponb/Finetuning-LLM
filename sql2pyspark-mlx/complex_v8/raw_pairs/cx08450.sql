WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE source = 'album'
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        listener_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(source_len) AS session_source_len,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM per_session s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device IS NOT NULL
        )
      AND s.session_start >= TIMESTAMP '2025-01-17 09:00:00'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(duration_min) IGNORE NULLS OVER (PARTITION BY YEAR(session_end), listener_id ORDER BY duration_min, listener_id, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_min,
        LAST_VALUE(session_source_len) IGNORE NULLS OVER (ORDER BY event_count ASC, listener_id, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_source_len
    FROM with_activity
)
SELECT
    listener_id,
    session_id,
    session_end,
    event_count,
    session_source_len,
    duration_min,
    first_known_duration_min
FROM with_windows
WHERE session_end < TIMESTAMP '2025-02-17 09:00:00'
ORDER BY event_count, session_source_len DESC

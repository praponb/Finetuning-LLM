WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d
    FROM plays
    WHERE track_id >= 16
),
with_prev AS (
    -- Previous played_at_plus_7d per track_id
    SELECT *, LAG(played_at_plus_7d) OVER (PARTITION BY track_id ORDER BY played_at_plus_7d, id) AS prev_played_at_plus_7d
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at_plus_7d IS NULL OR played_at_plus_7d > prev_played_at_plus_7d + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        played_at_plus_7d,
        starts_session,
        ROUND(ms_played / NULLIF(session_no, 0), 3) AS ms_played_per_session_no,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM sessionized
    WHERE source IN ('album', 'radio', 'playlist')
)
SELECT DISTINCT played_at_plus_7d
FROM scored

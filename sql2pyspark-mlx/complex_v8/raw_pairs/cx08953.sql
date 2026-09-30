WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        TO_DATE(played_at) AS played_at_date
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM sessionized s
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    track_id,
    track_release_date,
    DATE_ADD(track_release_date, 30) AS track_release_date_plus_30d,
    FLOOR(track_bpm / 25) * 25 AS track_bpm_bucket
FROM combined_details_track
ORDER BY track_release_date_plus_30d DESC

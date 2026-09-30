WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source IN ('playlist', 'album')
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        l.plan AS listener_plan,
        t.release_date AS track_release_date,
        t.duration_sec AS track_duration_sec
    FROM recent_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
scored AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(listener_plan)) AS listener_plan_len
    FROM with_parent
)
SELECT
    listener_plan,
    is_skipped,
    track_id,
    SUM(listener_plan_len) AS total_listener_plan_len,
    MAX(track_release_date) AS last_track_release_date
FROM scored
GROUP BY GROUPING SETS ((listener_plan, is_skipped, track_id), (listener_plan), (is_skipped))

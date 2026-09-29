WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE is_skipped = FALSE
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec,
        l.username AS listener_username,
        l.plan AS listener_plan
    FROM cleaned_plays s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(device, 'ALL') AS device,
    track_id,
    listener_id,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids,
    MAX(track_duration_sec) AS max_track_duration_sec,
    ROUND(STDDEV(track_duration_sec), 2) AS stddev_track_duration_sec
FROM combined_details
GROUP BY GROUPING SETS ((device, track_id, listener_id), (device), (track_id), ())
ORDER BY device NULLS LAST, track_id NULLS LAST, listener_id NULLS LAST

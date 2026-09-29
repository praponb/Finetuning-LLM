WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = FALSE
      AND listener_id >= 5
),
enriched_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.duration_sec < 309
)
SELECT id, metric_name, reading
FROM (
    SELECT id, track_duration_sec, ms_played
    FROM enriched_track
) src
UNPIVOT (
    reading FOR metric_name IN (track_duration_sec, ms_played)
) unp

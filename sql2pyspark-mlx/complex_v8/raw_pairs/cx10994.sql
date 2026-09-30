WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE ms_played < 308104
      AND track_id >= 14
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN device IN ('speaker', 'phone') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group
    FROM recent
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.played_at_label,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM annotated s
    JOIN plays p ON p.track_id = s.track_id AND p.listener_id IS NOT NULL
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.played_at_label,
        s.play_id,
        s.play_played_at,
        s.play_listener_id,
        t.bpm AS track_bpm,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec
    FROM detail_rows_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.duration_sec >= 209
)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, track_duration_sec, track_bpm
    FROM enriched_track
) src
UNPIVOT (
    reading FOR metric_name IN (track_duration_sec, track_bpm)
) unp

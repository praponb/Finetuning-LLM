WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE track_id >= 25
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.device_prefix,
        s.played_at_dayofweek,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.device_prefix,
        s.played_at_dayofweek,
        s.track_duration_sec,
        s.track_moods,
        t.artist_id AS track_artist_id,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit_2
    FROM with_parent s
    JOIN tracks t ON t.id = s.track_id
)
SELECT *
FROM (
    SELECT track_release_date, source, played_at_dayofweek
    FROM with_parent_track
) src
PIVOT (
    AVG(played_at_dayofweek)
    FOR source IN ('radio', 'album')
)

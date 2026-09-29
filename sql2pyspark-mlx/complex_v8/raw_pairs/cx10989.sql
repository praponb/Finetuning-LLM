WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE source IN ('radio', 'search', 'playlist')
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.device_prefix,
        s.ms_played_bucket,
        l.interests AS listener_interests,
        l.country AS listener_country,
        l.username AS listener_username,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date
    FROM filtered_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_parent_track AS (
    -- Attach track attributes
    SELECT s.*, t.is_explicit AS track_is_explicit
    FROM with_details_listener s
    JOIN tracks t ON t.id = s.track_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, track_release_date, listener_country, track_duration_sec
    FROM with_parent_track
) src
PIVOT (
    COUNT(track_duration_sec)
    FOR listener_country IN ('KR', 'BR', 'TH', 'UK')
)

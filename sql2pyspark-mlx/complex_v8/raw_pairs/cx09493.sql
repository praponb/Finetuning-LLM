WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE source IN ('search', 'album')
),
with_activity AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND SIZE(l.interests) >= 2
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        source,
        device_ref,
        source_clean,
        ROUND((UNIX_TIMESTAMP(played_at_hour) - UNIX_TIMESTAMP(played_at)) / 60, 1) AS minutes_played_at_to_played_at_hour,
        ROUND((UNIX_TIMESTAMP(played_at_hour) - UNIX_TIMESTAMP(played_at)) / 3600, 1) AS hours_played_at_to_played_at_hour,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM with_activity
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(source_clean), 1, 4) AS source_clean_prefix
    FROM labeled
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.device_ref,
        s.source_clean,
        s.minutes_played_at_to_played_at_hour,
        s.hours_played_at_to_played_at_hour,
        s.source_slug,
        s.source_clean_prefix,
        t.moods AS track_moods,
        t.release_date AS track_release_date,
        a.is_verified AS artist_is_verified
    FROM annotated s
    JOIN tracks t ON t.id = s.track_id AND t.is_explicit = TRUE
    LEFT JOIN artists a ON a.id = t.artist_id
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.source_slug,
        s.track_moods,
        s.track_release_date,
        s.artist_is_verified,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source
    FROM enriched s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT
    id,
    track_moods,
    track_release_date,
    artist_is_verified,
    play_id,
    COALESCE(play_device, 'UNKNOWN') AS play_device_or_unknown
FROM line_level_plays
WHERE play_device IS NOT NULL
ORDER BY play_device_or_unknown DESC, play_id

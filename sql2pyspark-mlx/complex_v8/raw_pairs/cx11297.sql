WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        CASE WHEN bpm >= 147 THEN 'large' WHEN bpm >= 96 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM tracks
    WHERE duration_sec > 164
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        p.id AS play_id,
        p.device AS play_device,
        p.listener_id AS play_listener_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM base s
    LEFT JOIN plays p ON p.track_id = s.id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id <= 19
              AND MONTH(p.played_at) IN (1, 2)
        )
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    play_listener_id,
    play_source,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
    INITCAP(TRIM(play_device)) AS play_device_clean
FROM filtered
ORDER BY play_device_clean DESC, id DESC NULLS FIRST

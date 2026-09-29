WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
      AND duration_sec BETWEEN 142 AND 352
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.title_slug,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device
    FROM recent s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    is_explicit,
    artist_id,
    COALESCE(play_device, 'ALL') AS play_device,
    MIN(play_ms_played) AS min_play_ms_played
FROM exploded_detail
GROUP BY CUBE(is_explicit, artist_id, play_device)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST, play_device NULLS LAST

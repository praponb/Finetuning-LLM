WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        ABS(duration_sec - 209) AS duration_sec_distance,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE is_explicit = TRUE
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.greatest_bpm_duration_sec,
        s.release_date_year,
        a.country AS artist_country
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.played_at AS play_played_at
    FROM joined s
    LEFT JOIN plays p ON p.track_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, play_device, duration_sec
    FROM exploded_detail
) src
PIVOT (
    SUM(duration_sec)
    FOR play_device IN ('tv', 'phone', 'speaker')
)

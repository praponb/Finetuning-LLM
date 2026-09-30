WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE (release_date < DATE '2025-08-07' OR duration_sec > 184)
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.title_len,
        s.release_date_quarter,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.release_date_quarter,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM enriched_artist s
    LEFT JOIN plays p ON p.track_id = s.id
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.country AS artist_country,
        a.artist_name,
        p.source AS play_source_2,
        p.ms_played AS play_ms_played
    FROM exploded_detail s
    JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'L%'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT is_explicit, artist_country, play_source, bpm
    FROM with_parent_artist
) src
PIVOT (
    SUM(bpm)
    FOR play_source IN ('playlist', 'radio', 'album', 'search')
)

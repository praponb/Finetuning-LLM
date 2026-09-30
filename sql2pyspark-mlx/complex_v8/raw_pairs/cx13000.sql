WITH recent_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE (NOT is_verified OR debut_year <= 2018)
),
line_level AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.genre_or_unknown,
        t.id AS track_id,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date
    FROM recent_artists s
    JOIN tracks t ON t.artist_id = s.id AND t.is_explicit = FALSE
),
enriched AS (
    -- Join tracks
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.track_id,
        s.track_duration_sec,
        s.track_is_explicit,
        s.track_release_date,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec_2,
        t.release_date AS track_release_date_2
    FROM line_level s
    JOIN tracks t ON t.id = s.track_id
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM enriched s
    WHERE s.track_artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE SIZE(t.moods) >= 2
        )
)
-- Final projection
SELECT
    id,
    artist_name,
    country,
    track_id,
    track_is_explicit,
    track_release_date,
    track_artist_id,
    DATE_TRUNC('MONTH', track_release_date) AS track_release_date_month
FROM without_matches
WHERE track_duration_sec > 144

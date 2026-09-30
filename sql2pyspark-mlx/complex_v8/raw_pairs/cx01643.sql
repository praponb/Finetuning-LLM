WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        SUBSTRING(TRIM(country), 1, 3) AS country_prefix,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE UPPER(TRIM(artist_name)) LIKE 'P%'
),
detail_rows_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.country,
        s.is_verified,
        s.country_prefix,
        s.debut_year_filled,
        t.id AS track_id,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec,
        t.bpm AS track_bpm,
        t.moods AS track_moods
    FROM base_artists s
    JOIN tracks t ON t.artist_id = s.id
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.*,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit_2,
        t.artist_id AS track_artist_id,
        a.country AS artist_country
    FROM detail_rows_tracks s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        t.release_date AS track_release_date_2
    FROM combined_details_track s
    LEFT JOIN artists a ON a.id = s.track_artist_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
exploded AS (
    -- One row per track_mood
    SELECT
        s.id,
        s.country,
        s.is_verified,
        s.country_prefix,
        s.debut_year_filled,
        s.track_id,
        s.track_is_explicit,
        s.track_duration_sec,
        s.track_bpm,
        s.track_release_date,
        s.track_is_explicit_2,
        s.track_artist_id,
        s.artist_country,
        s.artist_is_verified,
        s.track_release_date_2,
        t.track_mood
    FROM with_parent_artist s
    LATERAL VIEW EXPLODE(s.track_moods) t AS track_mood
)
SELECT
    id,
    country,
    is_verified,
    country_prefix,
    debut_year_filled,
    track_id,
    track_is_explicit,
    track_duration_sec,
    track_bpm,
    track_is_explicit_2,
    artist_is_verified,
    track_release_date_2,
    track_mood,
    ADD_MONTHS(track_release_date, 3) AS track_release_date_plus_3m,
    DATEDIFF(track_release_date_2, track_release_date) AS days_track_release_date_to_track_release_date_2
FROM exploded

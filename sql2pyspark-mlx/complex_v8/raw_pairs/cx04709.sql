WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE duration_sec <= 353
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.genre AS artist_genre,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        artist_genre,
        artist_name,
        artist_debut_year,
        artist_debut_year - duration_sec AS artist_debut_year_minus_duration_sec,
        DATE_ADD(release_date, 7) AS release_date_plus_7d,
        CONCAT_WS(': ', artist_genre, artist_name) AS artist_genre_artist_name_label
    FROM combined_details_artist
    WHERE YEAR(release_date) = 2024
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        artist_genre,
        artist_name,
        artist_debut_year,
        artist_debut_year_minus_duration_sec,
        release_date_plus_7d,
        artist_genre_artist_name_label,
        CONCAT_WS(' / ', artist_name, artist_genre) AS artist_name_artist_genre_label
    FROM labeled
),
artist_id_stats AS (
    -- Max duration_sec per artist_id
    SELECT artist_id, MAX(duration_sec) AS max_duration_sec
    FROM tagged
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_duration_sec,
        ROUND(s.duration_sec - g.max_duration_sec, 2) AS duration_sec_minus_max
    FROM tagged s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec = g.max_duration_sec
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    artist_genre,
    artist_name,
    artist_debut_year,
    artist_debut_year_minus_duration_sec,
    release_date_plus_7d,
    artist_genre_artist_name_label,
    artist_name_artist_genre_label,
    duration_sec_minus_max,
    MONTH(release_date) AS release_date_month,
    DATEDIFF(DATE '2025-12-31', release_date_plus_7d) AS days_since_release_date_plus_7d
FROM outliers
WHERE artist_debut_year >= 1991

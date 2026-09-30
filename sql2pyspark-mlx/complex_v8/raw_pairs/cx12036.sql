WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        DATE_ADD(release_date, 7) AS release_date_plus_7d,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE bpm <= 127
      AND release_date >= DATE '2024-12-03'
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        s.release_date_plus_7d,
        s.duration_sec_filled,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM recent s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year BETWEEN 1996 AND 2015
),
with_parent_artist AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM with_details_artist s
    LEFT JOIN artists a ON a.id = s.artist_id
),
latest AS (
    -- Keep the latest row per artist_id, artist_genre
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        days_since_release_date,
        release_date_plus_7d,
        duration_sec_filled,
        artist_name,
        artist_debut_year,
        artist_genre
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id, artist_genre ORDER BY bpm DESC, id DESC) AS rn
        FROM with_parent_artist
    ) ranked
    WHERE rn = 1
)
SELECT
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    artist_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
    ROUND(AVG(days_since_release_date), 2) AS avg_days_since_release_date
FROM latest
GROUP BY GROUPING SETS ((artist_genre, artist_id), (artist_genre), (artist_id), ())
ORDER BY artist_genre NULLS LAST, artist_id NULLS LAST

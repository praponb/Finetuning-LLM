WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec >= 183
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM filtered_tracks
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM filtered_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm_minus_avg,
        a.genre AS artist_genre
    FROM vs_group s
    JOIN artists a ON a.id = s.artist_id AND a.country IN ('US', 'BR', 'TH')
    WHERE a.genre = 'Pop'
),
artist_genre_stats AS (
    -- Avg bpm_minus_avg per artist_genre
    SELECT artist_genre, AVG(bpm_minus_avg) AS avg_bpm_minus_avg
    FROM with_details
    GROUP BY artist_genre
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_minus_avg,
        ROUND(s.bpm_minus_avg - g.avg_bpm_minus_avg, 2) AS bpm_minus_avg_minus_avg
    FROM with_details s
    JOIN artist_genre_stats g ON g.artist_genre = s.artist_genre
    WHERE s.bpm_minus_avg > g.avg_bpm_minus_avg
)
SELECT
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    artist_id,
    COUNT(*) AS row_count
FROM above_average
GROUP BY GROUPING SETS ((artist_genre, artist_id), (artist_genre))

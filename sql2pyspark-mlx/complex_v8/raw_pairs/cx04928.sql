WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
artist_id_stats AS (
    -- Avg artist_debut_year per artist_id
    SELECT artist_id, AVG(artist_debut_year) AS avg_artist_debut_year
    FROM joined
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_artist_debut_year,
        ROUND(s.artist_debut_year - g.avg_artist_debut_year, 2) AS artist_debut_year_minus_avg
    FROM joined s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.artist_debut_year > g.avg_artist_debut_year
),
artist_id_stats_2 AS (
    -- Avg artist_debut_year per artist_id
    SELECT artist_id, AVG(artist_debut_year) AS avg_artist_debut_year_2
    FROM vs_group
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_artist_debut_year_2,
        ROUND(s.artist_debut_year - g.avg_artist_debut_year_2, 2) AS artist_debut_year_minus_avg_2
    FROM vs_group s
    JOIN artist_id_stats_2 g ON g.artist_id = s.artist_id
    WHERE s.artist_debut_year > g.avg_artist_debut_year_2
)
SELECT DISTINCT bpm, artist_id
FROM above_average
ORDER BY bpm, artist_id DESC

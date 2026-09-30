WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id AND a.genre NOT IN ('Pop', 'Jazz')
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM enriched_artist
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM enriched_artist s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(max_bpm) OVER w_artist_id AS artist_id_min_max_bpm,
        ROUND(max_bpm - AVG(max_bpm) OVER (PARTITION BY artist_id), 2) AS max_bpm_vs_artist_id_avg
    FROM compared
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.artist_is_verified,
        s.artist_name,
        s.artist_debut_year,
        s.max_bpm,
        s.bpm_minus_max,
        s.artist_id_min_max_bpm,
        s.max_bpm_vs_artist_id_avg,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM with_windows s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    artist_is_verified,
    artist_name,
    artist_debut_year,
    max_bpm,
    bpm_minus_max,
    artist_id_min_max_bpm,
    max_bpm_vs_artist_id_avg,
    artist_country,
    artist_genre,
    DATE_TRUNC('MONTH', release_date) AS release_date_month
FROM combined_details
ORDER BY max_bpm_vs_artist_id_avg DESC, artist_name DESC

WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE is_explicit = TRUE
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.moods_text,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year > 1994
)
SELECT
    is_explicit,
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN artist_genre = 'HipHop' THEN artist_debut_year ELSE 0 END) AS artist_debut_year_artist_genre_hiphop,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_details
GROUP BY ROLLUP(is_explicit, artist_genre)
ORDER BY is_explicit NULLS LAST, artist_genre NULLS LAST

WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE bpm > (SELECT AVG(bpm) FROM cleaned)
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM without_matches s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year >= 1991
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY artist_debut_year DESC NULLS FIRST, id) AS artist_debut_year_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC NULLS FIRST), 3) AS artist_debut_year_percent_rank,
        DENSE_RANK() OVER (ORDER BY artist_debut_year DESC NULLS FIRST) AS artist_debut_year_dense_rank_2,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_id, artist_country)) AS distinct_artist_genre_in_artist_id_artist_countr
    FROM joined_artist
)
SELECT id, measure, metric_value
FROM (
    SELECT id, distinct_artist_genre_in_artist_id_artist_countr, artist_debut_year_dense_rank
    FROM windowed
) src
UNPIVOT (
    metric_value FOR measure IN (distinct_artist_genre_in_artist_id_artist_countr, artist_debut_year_dense_rank)
) unp
ORDER BY id, measure

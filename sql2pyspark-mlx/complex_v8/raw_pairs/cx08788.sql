WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-06-03' AND DATE '2025-08-28'
      AND ARRAY_CONTAINS(moods, 'sad')
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM base_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre NOT IN ('Jazz', 'Indie')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        release_date,
        artist_genre,
        artist_debut_year,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_id)) AS distinct_artist_genre_in_artist_id,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_genre) AS rows_in_is_explicit_artist_genre,
        RANK() OVER (PARTITION BY artist_genre ORDER BY release_date DESC, id) AS release_date_rank,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY artist_genre ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER w_is_explicit_artist_genre, 2) AS artist_debut_year_vs_is_explicit_artist_genre_av,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY artist_id), 0), 4) AS artist_debut_year_share_of_artist_id,
        ROW_NUMBER() OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec, id) AS rn,
        MIN(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        LAG(artist_debut_year) OVER w_artist_id_artist_genre_by_re AS prev_artist_debut_year,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY release_date) AS release_date_rank_2
    FROM combined_details_artist
    WINDOW w_is_explicit_artist_genre AS (PARTITION BY is_explicit, artist_genre), w_artist_id_artist_genre_by_re AS (PARTITION BY artist_id, artist_genre ORDER BY release_date, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE rn = 1
      AND release_date >= DATE '2024-09-26'
)
SELECT
    id,
    duration_sec,
    release_date,
    artist_genre,
    artist_debut_year,
    distinct_artist_genre_in_artist_id,
    release_date_rank,
    artist_debut_year_vs_is_explicit_artist_genre_av,
    min_duration_sec_so_far,
    second_artist_debut_year,
    release_date_rank_2,
    UPPER(TRIM(artist_genre)) AS artist_genre_clean,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM first_rows
WHERE last_known_artist_debut_year <= 2016
ORDER BY release_date_rank_2 NULLS LAST, artist_debut_year DESC

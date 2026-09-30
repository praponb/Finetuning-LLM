WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods
    FROM tracks
    WHERE bpm > 76
      AND YEAR(release_date) = 2024
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND NOT p.is_skipped
        )
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        a.artist_name,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM with_activity s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY release_date NULLS LAST), 3) AS release_date_cume_dist
    FROM joined_artist
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        s.artist_name,
        s.artist_is_verified,
        s.release_date_cume_dist,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified_2,
        a.debut_year AS artist_debut_year
    FROM sequenced s
    JOIN artists a ON a.id = s.artist_id
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods,
        artist_name,
        release_date_cume_dist,
        artist_debut_year,
        LAG(artist_debut_year, 1, 0) OVER w_artist_id_artist_genre_by_re AS prev_artist_debut_year,
        SUM(release_date_cume_dist) OVER (PARTITION BY MONTH(release_date), artist_is_verified_2) AS release_date_month_artist_is_verified_2_sum_rele,
        ROUND(release_date_cume_dist - AVG(release_date_cume_dist) OVER (PARTITION BY MONTH(release_date), artist_is_verified_2), 2) AS release_date_cume_dist_vs_release_date_month_art,
        ROW_NUMBER() OVER (PARTITION BY artist_id, artist_genre ORDER BY release_date DESC, id) AS seq_no,
        MAX(artist_debut_year) OVER (PARTITION BY artist_id, artist_genre ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS artist_debut_year_max_1_prior
    FROM enriched_artist
    WINDOW w_artist_id_artist_genre_by_re AS (PARTITION BY artist_id, artist_genre ORDER BY release_date DESC, id)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    moods,
    artist_name,
    release_date_cume_dist,
    artist_debut_year,
    prev_artist_debut_year,
    release_date_month_artist_is_verified_2_sum_rele,
    release_date_cume_dist_vs_release_date_month_art,
    seq_no,
    artist_debut_year_max_1_prior
FROM sequenced_2
WHERE title IS NOT NULL

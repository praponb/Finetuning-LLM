WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        ARRAY_CONTAINS(moods, 'chill') AS has_chill,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE duration_sec > 239
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.artist_name
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_genre), 2) AS artist_debut_year_vs_artist_genre_avg,
        ROW_NUMBER() OVER (ORDER BY release_date DESC, id) AS seq_no,
        SUM(bpm) OVER w_artist_genre_artist_id AS artist_genre_artist_id_sum_bpm,
        ROUND(CUME_DIST() OVER w_all_by_release_date_2, 3) AS release_date_cume_dist,
        ROUND(AVG(bpm_per_duration_sec) OVER w_artist_genre_artist_id, 2) AS artist_genre_artist_id_avg_bpm_per_duration_sec,
        ROUND(bpm_per_duration_sec - AVG(bpm_per_duration_sec) OVER (PARTITION BY artist_genre, artist_id), 2) AS bpm_per_duration_sec_vs_artist_genre_artist_id_a,
        RANK() OVER (PARTITION BY has_chill ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_rank,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        LEAD(duration_sec) OVER (ORDER BY release_date, id) AS next_duration_sec,
        SUM(duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_1_prior,
        MAX(bpm) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_max_bpm
    FROM with_details_artist
    WINDOW w_artist_genre_artist_id AS (PARTITION BY artist_genre, artist_id), w_all_by_release_date_2 AS (ORDER BY release_date, id)
)
SELECT DISTINCT id, next_duration_sec, artist_genre
FROM metrics
WHERE next_duration_sec >= 144
ORDER BY id, artist_genre

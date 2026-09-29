WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE release_date >= DATE '2024-08-26'
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.bpm_bucket,
        a.genre AS artist_genre,
        a.artist_name
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(release_date, 2) OVER (ORDER BY duration_sec DESC, id) AS next2_release_date,
        LEAD(duration_sec, 1, 0) OVER (ORDER BY duration_sec DESC, id) AS next_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY artist_genre, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, artist_genre ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(PERCENT_RANK() OVER w_artist_genre_artist_id_by_re_2, 3) AS release_date_percent_rank,
        DENSE_RANK() OVER (PARTITION BY artist_genre, artist_id ORDER BY release_date) AS release_date_dense_rank,
        RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_rank,
        COUNT(*) OVER (PARTITION BY artist_id, artist_genre) AS rows_in_artist_id_artist_genre
    FROM joined
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_artist_genre_artist_id_by_re_2 AS (PARTITION BY artist_genre, artist_id ORDER BY release_date, id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        bpm_bucket,
        artist_genre,
        artist_name,
        next2_release_date,
        next_duration_sec,
        min_duration_sec_so_far,
        duration_sec_avg_7,
        first_known_release_date,
        release_date_percent_rank,
        release_date_dense_rank,
        rows_in_artist_id_artist_genre
    FROM with_trends
    WHERE duration_sec_rank <= 5
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    moods,
    artist_genre,
    artist_name,
    next2_release_date,
    next_duration_sec,
    min_duration_sec_so_far,
    first_known_release_date,
    release_date_percent_rank,
    release_date_dense_rank,
    ROUND(min_duration_sec_so_far / NULLIF(duration_sec, 0), 3) AS min_duration_sec_so_far_per_duration_sec,
    UPPER(TRIM(artist_genre)) AS artist_genre_clean
FROM leaders

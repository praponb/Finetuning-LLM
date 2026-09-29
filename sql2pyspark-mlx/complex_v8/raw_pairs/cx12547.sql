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
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE duration_sec < 344
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.artist_name
    FROM valid_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY bpm) AS bpm_dense_rank,
        ROUND(artist_debut_year - AVG(artist_debut_year) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), 2) AS artist_debut_year_vs_artist_id_release_date_mont,
        COUNT(*) OVER (PARTITION BY artist_id, artist_genre) AS rows_in_artist_id_artist_genre,
        SIZE(COLLECT_SET(artist_genre) OVER w_artist_id_release_date_month) AS distinct_artist_genre_in_artist_id_release_date_,
        COUNT(*) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)) AS rows_in_artist_id_release_date_month,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id, artist_genre), 2) AS artist_id_artist_genre_avg_artist_debut_year,
        RANK() OVER (PARTITION BY artist_genre, artist_id ORDER BY artist_debut_year DESC NULLS FIRST, id) AS artist_debut_year_rank,
        ROUND(PERCENT_RANK() OVER w_artist_id_release_date_month_2, 3) AS bpm_percent_rank,
        LAG(bpm, 2) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm, id) AS prev2_bpm,
        RANK() OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm) AS bpm_rank,
        DENSE_RANK() OVER w_all_by_bpm AS bpm_dense_rank_2,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, artist_genre), 0), 4) AS bpm_share_of_artist_id_artist_genre
    FROM combined_details
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), w_artist_id_artist_genre AS (PARTITION BY artist_id, artist_genre), w_artist_id_release_date_month_2 AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY bpm, id), w_all_by_bpm AS (ORDER BY bpm, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        bpm_dense_rank,
        artist_id,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        ROUND(AVG(bpm_rank), 2) AS avg_bpm_rank,
        SUM(rows_in_artist_id_release_date_month) AS total_rows_in_artist_id_release_date_month,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        PERCENTILE_APPROX(rows_in_artist_id_release_date_month, 0.5) AS median_rows_in_artist_id_release_date_month,
        PERCENTILE_APPROX(bpm_rank, 0.5) AS median_bpm_rank,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY bpm_dense_rank, artist_id, DATE_TRUNC('MONTH', release_date)
)
-- Final projection
SELECT
    bpm_dense_rank,
    artist_id,
    release_date_span_days,
    median_rows_in_artist_id_release_date_month,
    cnt
FROM grouped
ORDER BY cnt DESC

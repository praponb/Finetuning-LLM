WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec) AS duration_sec_dense_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_release_date_month, 2) AS duration_sec_vs_artist_id_release_date_month_avg,
        ROUND(AVG(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior,
        NTILE(3) OVER w_artist_id_release_date_year AS duration_sec_tercile,
        ROW_NUMBER() OVER (PARTITION BY artist_id, MONTH(release_date) ORDER BY release_date DESC NULLS FIRST, id) AS release_date_seq,
        SUM(duration_sec) OVER w_artist_id_release_date_month AS artist_id_release_date_month_sum_duration_sec,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        RANK() OVER (PARTITION BY artist_id ORDER BY release_date) AS release_date_rank,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        LAG(release_date) OVER w_artist_id_release_date_month_2 AS prev_release_date,
        ROUND(AVG(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_2_prior
    FROM valid_tracks
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec, id), w_artist_id_release_date_month_2 AS (PARTITION BY artist_id, MONTH(release_date) ORDER BY release_date DESC NULLS FIRST, id)
),
scored AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(title_prefix), 1, 4) AS title_prefix_prefix
    FROM windowed
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    moods,
    title_prefix,
    duration_sec_vs_artist_id_release_date_month_avg,
    duration_sec_avg_6_prior,
    duration_sec_tercile,
    release_date_seq,
    last_duration_sec,
    release_date_rank,
    max_duration_sec_so_far,
    prev_release_date,
    duration_sec_avg_2_prior,
    title_prefix_prefix,
    CONCAT(title_prefix, '#', CAST(artist_id AS STRING)) AS title_prefix_ref
FROM scored
WHERE last_duration_sec < 312
ORDER BY release_date_seq DESC

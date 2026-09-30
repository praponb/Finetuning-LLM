WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE duration_sec < 409
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        ROUND(PERCENT_RANK() OVER w_artist_id_release_date_year, 3) AS duration_sec_percent_rank,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS prev_duration_sec,
        MIN(bpm) OVER (PARTITION BY artist_id) AS artist_id_min_bpm,
        LEAD(bpm) OVER w_artist_id_by_bpm AS next_bpm,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC) AS duration_sec_dense_rank
    FROM prepared_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec DESC), w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (ORDER BY duration_sec_vs_artist_id_avg DESC, id) AS duration_sec_vs_artist_id_avg_tercile,
        MAX(first_duration_sec) OVER w_release_date_month AS release_date_month_max_first_duration_sec,
        ROUND(artist_id_max_bpm / NULLIF(SUM(artist_id_max_bpm) OVER w_release_date_month, 0), 4) AS artist_id_max_bpm_share_of_release_date_month
    FROM windowed
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date))
)
SELECT
    id,
    artist_id,
    duration_sec,
    bpm,
    moods,
    release_date_label,
    artist_id_max_bpm,
    duration_sec_share_of_artist_id,
    second_bpm,
    bpm_share_of_artist_id,
    rows_in_artist_id,
    first_duration_sec,
    next_bpm,
    duration_sec_dense_rank,
    artist_id_max_bpm_share_of_release_date_month,
    LOWER(TRIM(release_date_label)) AS release_date_label_clean,
    ROUND(duration_sec / NULLIF(duration_sec_dense_rank, 0), 3) AS duration_sec_per_duration_sec_dense_rank
FROM ranked
ORDER BY second_bpm DESC NULLS FIRST

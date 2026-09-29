WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE artist_id >= 8
      AND UPPER(TRIM(title)) LIKE 'E%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_bpm,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY bpm DESC, id)) AS days_since_prev_release_date,
        MIN(bpm) OVER (PARTITION BY artist_id) AS artist_id_min_bpm,
        bpm - LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm NULLS LAST, id) AS bpm_change,
        SUM(bpm) OVER w_artist_id AS artist_id_sum_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        ROUND(PERCENT_RANK() OVER w_is_explicit_artist_id_by_bpm_2, 3) AS bpm_percent_rank,
        LEAD(release_date) OVER (ORDER BY bpm DESC, id) AS next_release_date,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_release_date_ti, 3) AS release_date_percent_rank,
        DENSE_RANK() OVER w_is_explicit_release_date_mon AS release_date_dense_rank,
        LAG(duration_sec, 1, 0) OVER w_is_explicit_release_date_mon AS prev_duration_sec,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm
    FROM filtered
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_is_explicit_artist_id_by_bpm_2 AS (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC), w_artist_id_by_release_date_ti AS (PARTITION BY artist_id ORDER BY release_date NULLS LAST), w_is_explicit_release_date_mon AS (PARTITION BY is_explicit, MONTH(release_date) ORDER BY release_date ASC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        release_date_label,
        is_explicit_artist_id_max_bpm,
        second_duration_sec,
        artist_id_min_bpm,
        artist_id_sum_bpm,
        bpm_avg_2,
        release_date_dense_rank,
        GREATEST(bpm_avg_2, duration_sec) AS greatest_bpm_avg_2_duration_sec,
        GREATEST(bpm_avg_2, artist_id_sum_bpm) AS greatest_bpm_avg_2_artist_id_sum_bpm,
        MONTH(next_release_date) AS next_release_date_month
    FROM metrics
    WHERE second_duration_sec >= 142
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        bpm,
        moods,
        release_date_label,
        second_duration_sec,
        artist_id_sum_bpm,
        bpm_avg_2,
        release_date_dense_rank,
        greatest_bpm_avg_2_artist_id_sum_bpm,
        next_release_date_month,
        SUM(bpm_avg_2) OVER (PARTITION BY artist_id, release_date_dense_rank) AS artist_id_release_date_dense_rank_sum_bpm_avg_2,
        LAST_VALUE(release_date_dense_rank) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_dense_rank,
        NTILE(10) OVER (ORDER BY bpm, id) AS bpm_decile,
        ROUND(AVG(next_release_date_month) OVER w_artist_id_release_date_dense, 2) AS artist_id_release_date_dense_rank_avg_next_relea,
        RANK() OVER w_artist_id_by_second_duration AS second_duration_sec_rank,
        SUM(release_date_dense_rank) OVER w_artist_id_release_date_dense AS artist_id_release_date_dense_rank_sum_release_da
    FROM scored
    WINDOW w_artist_id_release_date_dense AS (PARTITION BY artist_id, release_date_dense_rank), w_artist_id_by_second_duration AS (PARTITION BY artist_id ORDER BY second_duration_sec DESC)
),
grouped AS (
    -- Group-level totals
    SELECT
        next_release_date_month,
        artist_id,
        last_release_date_dense_rank,
        ROUND(SUM(last_release_date_dense_rank) / NULLIF(SUM(second_duration_sec_rank), 0), 3) AS last_release_date_dense_rank_to_second_duration_,
        MIN(next_release_date_month) AS min_next_release_date_month,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(artist_id_release_date_dense_rank_sum_bpm_avg_2), 2) AS stddev_artist_id_release_date_dense_rank_sum_bpm,
        ROUND(STDDEV(release_date_dense_rank), 2) AS stddev_release_date_dense_rank,
        MAX(next_release_date_month) AS max_next_release_date_month
    FROM with_trends
    WHERE artist_id_release_date_dense_rank_sum_release_da BETWEEN 31 AND 60
    GROUP BY next_release_date_month, artist_id, last_release_date_dense_rank
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    next_release_date_month,
    artist_id,
    min_next_release_date_month,
    n_rows,
    stddev_artist_id_release_date_dense_rank_sum_bpm,
    stddev_release_date_dense_rank,
    max_next_release_date_month,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
    ABS(n_rows - 5) AS n_rows_distance
FROM grouped
ORDER BY next_release_date_month NULLS LAST, n_rows_bucket DESC NULLS FIRST

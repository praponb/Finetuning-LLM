WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 352
      AND release_date >= DATE '2024-11-14'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(release_date) OVER (ORDER BY duration_sec, id) AS prev_release_date,
        SUM(duration_sec) OVER (PARTITION BY title_len, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        greatest_duration_sec_bpm - LAG(greatest_duration_sec_bpm) OVER w_all_by_duration_sec AS greatest_duration_sec_bpm_change,
        NTILE(3) OVER w_all_by_duration_sec AS duration_sec_tercile,
        NTH_VALUE(title_len, 2) OVER (PARTITION BY title_len, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_title_len,
        DENSE_RANK() OVER w_title_len_artist_id_by_relea_3 AS release_date_dense_rank
    FROM recent_tracks
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_title_len_artist_id_by_relea_3 AS (PARTITION BY title_len, artist_id ORDER BY release_date)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        moods,
        title_len,
        greatest_duration_sec_bpm,
        running_duration_sec,
        greatest_duration_sec_bpm_change,
        duration_sec_tercile,
        second_title_len,
        release_date_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY title_len ORDER BY greatest_duration_sec_bpm DESC), 3) AS greatest_duration_sec_bpm_cume_dist,
        ROUND(AVG(release_date_dense_rank) OVER (PARTITION BY title_len ORDER BY release_date_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS release_date_dense_rank_avg_7,
        LEAD(title_len, 2) OVER (PARTITION BY title_len ORDER BY greatest_duration_sec_bpm DESC, id) AS next2_title_len
    FROM sequenced
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        moods,
        title_len,
        greatest_duration_sec_bpm,
        running_duration_sec,
        greatest_duration_sec_bpm_change,
        duration_sec_tercile,
        second_title_len,
        greatest_duration_sec_bpm_cume_dist,
        release_date_dense_rank_avg_7,
        next2_title_len
    FROM ranked
    WHERE release_date_dense_rank <= 3
      AND duration_sec BETWEEN 165 AND 352
)
-- Subtotals with GROUPING SETS
SELECT
    next2_title_len,
    second_title_len,
    title_len,
    GROUPING(next2_title_len) AS is_total_next2_title_len,
    MAX(duration_sec) AS max_duration_sec
FROM first_rows
GROUP BY GROUPING SETS ((next2_title_len, second_title_len, title_len), (next2_title_len), (second_title_len), ())
ORDER BY next2_title_len NULLS LAST, second_title_len NULLS LAST, title_len NULLS LAST

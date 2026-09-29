WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'P%' OR bpm BETWEEN 75 AND 155)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE duration_sec > (SELECT MIN(duration_sec) FROM recent)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        release_date_label,
        NTILE(10) OVER (ORDER BY duration_sec, id) AS duration_sec_decile,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        MIN(bpm) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far
    FROM screened
),
stats AS (
    -- Summarise per group
    SELECT
        artist_id,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows,
        MIN(second_duration_sec) AS min_second_duration_sec,
        ROUND(SUM(second_duration_sec) / NULLIF(SUM(duration_sec_decile), 0), 3) AS second_duration_sec_to_duration_sec_decile_ratio
    FROM windowed
    WHERE duration_sec_decile <= 9
    GROUP BY artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        artist_id,
        distinct_ids,
        n_rows,
        min_second_duration_sec,
        second_duration_sec_to_duration_sec_decile_ratio,
        SUM(min_second_duration_sec) OVER (PARTITION BY n_rows ORDER BY second_duration_sec_to_duration_sec_decile_ratio DESC, artist_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_second_duration_sec_sum_2,
        MIN(distinct_ids) OVER (PARTITION BY n_rows ORDER BY second_duration_sec_to_duration_sec_decile_ratio DESC, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_distinct_ids_so_far
    FROM stats
),
detail_rows AS (
    -- One row per track
    SELECT
        s.artist_id,
        s.distinct_ids,
        s.n_rows,
        s.min_second_duration_sec,
        s.second_duration_sec_to_duration_sec_decile_ratio,
        s.min_second_duration_sec_sum_2,
        s.min_distinct_ids_so_far,
        t.id AS track_id,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM with_trends s
    JOIN tracks t ON t.artist_id = s.artist_id
)
SELECT
    n_rows,
    track_is_explicit,
    MAX(second_duration_sec_to_duration_sec_decile_ratio) AS max_second_duration_sec_to_duration_sec_decile_r,
    COUNT(DISTINCT track_id) AS distinct_track_ids
FROM detail_rows
GROUP BY GROUPING SETS ((n_rows, track_is_explicit), (n_rows), ())
ORDER BY n_rows NULLS LAST, track_is_explicit NULLS LAST

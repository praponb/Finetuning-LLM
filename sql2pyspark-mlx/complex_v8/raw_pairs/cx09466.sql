WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        bpm + duration_sec AS bpm_plus_duration_sec,
        ARRAY_CONTAINS(moods, 'chill') AS has_chill
    FROM tracks
    WHERE artist_id >= 10
      AND duration_sec <= 352
),
totals AS (
    -- Summarise per group
    SELECT
        artist_id,
        ROUND(STDDEV(bpm), 2) AS stddev_bpm,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN NOT is_explicit THEN bpm_plus_duration_sec ELSE 0 END) AS bpm_plus_duration_sec_not_is_explicit,
        ROUND(AVG(bpm_plus_duration_sec), 2) AS avg_bpm_plus_duration_sec
    FROM recent_tracks
    WHERE release_date < DATE '2025-08-02'
    GROUP BY artist_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY cnt DESC, artist_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_ids_sum_3,
        DENSE_RANK() OVER (ORDER BY stddev_bpm ASC) AS stddev_bpm_dense_rank,
        LEAD(avg_bpm_plus_duration_sec, 3, 0) OVER (PARTITION BY distinct_ids ORDER BY cnt DESC, artist_id) AS next3_avg_bpm_plus_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY distinct_ids ORDER BY cnt DESC, artist_id) AS rn,
        MAX(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY cnt DESC, artist_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS distinct_ids_max_2,
        MAX(stddev_bpm) OVER (PARTITION BY distinct_ids, cnt) AS distinct_ids_cnt_max_stddev_bpm
    FROM totals
),
with_children AS (
    -- One row per track
    SELECT
        s.artist_id,
        s.bpm_plus_duration_sec_not_is_explicit,
        s.stddev_bpm_dense_rank,
        s.next3_avg_bpm_plus_duration_sec,
        s.rn,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.duration_sec AS track_duration_sec
    FROM ranked s
    JOIN tracks t ON t.artist_id = s.artist_id AND t.release_date IS NOT NULL
)
-- Final projection
SELECT
    artist_id,
    bpm_plus_duration_sec_not_is_explicit,
    stddev_bpm_dense_rank,
    next3_avg_bpm_plus_duration_sec,
    rn,
    track_id,
    track_release_date,
    track_duration_sec,
    ABS(track_duration_sec - 271) AS track_duration_sec_distance,
    DATEDIFF(DATE '2025-12-31', track_release_date) AS days_since_track_release_date
FROM with_children
ORDER BY bpm_plus_duration_sec_not_is_explicit, rn DESC

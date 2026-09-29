WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        ABS(bpm - 96) AS bpm_distance,
        LENGTH(TRIM(title)) AS title_len,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE bpm > 76
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.is_skipped = TRUE THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM cleaned s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        bpm,
        moods,
        bpm_distance,
        title_clean,
        play_is_skipped_count,
        ROUND(AVG(title_len) OVER (PARTITION BY artist_id, play_is_skipped_count ORDER BY last_played_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS title_len_avg_2_prior,
        SUM(bpm_distance) OVER (PARTITION BY artist_id, title_len) AS artist_id_title_len_sum_bpm_distance,
        ROUND(bpm_distance - AVG(bpm_distance) OVER (PARTITION BY artist_id, title_len), 2) AS bpm_distance_vs_artist_id_title_len_avg,
        ROW_NUMBER() OVER (PARTITION BY artist_id, play_is_skipped_count ORDER BY last_played_at, id) AS last_played_at_seq
    FROM with_counts
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        last_played_at_seq,
        play_is_skipped_count,
        ROUND(STDDEV(play_is_skipped_count), 2) AS stddev_play_is_skipped_count,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(play_is_skipped_count, 0.5) AS median_play_is_skipped_count,
        PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec
    FROM with_windows
    GROUP BY 1, 2
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM per_group)
)
-- Final projection
SELECT
    last_played_at_seq,
    play_is_skipped_count,
    stddev_play_is_skipped_count,
    median_play_is_skipped_count
FROM qualified
ORDER BY play_is_skipped_count

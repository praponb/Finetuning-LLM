WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE SIZE(moods) >= 1
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.release_date_label,
        s.duration_sec_bucket,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at
    FROM valid s
    JOIN plays p ON p.track_id = s.id AND p.listener_id <= 17
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(duration_sec_bucket, 0) AS duration_sec_bucket_filled,
        IF(play_is_skipped, 'yes', 'no') AS play_is_skipped_label,
        CASE WHEN play_is_skipped THEN 1 ELSE 0 END AS play_is_skipped_flag
    FROM with_children
),
deduped AS (
    -- Keep the latest row per play_listener_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        release_date_label,
        duration_sec_bucket,
        play_id,
        play_is_skipped,
        play_listener_id,
        play_played_at,
        duration_sec_bucket_filled,
        play_is_skipped_label,
        play_is_skipped_flag
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_listener_id ORDER BY duration_sec_bucket DESC, id DESC, play_id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
),
latest_per_key AS (
    -- Keep the latest row per artist_id, play_is_skipped_label
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        release_date_label,
        duration_sec_bucket,
        play_id,
        play_is_skipped,
        play_listener_id,
        play_played_at,
        duration_sec_bucket_filled,
        play_is_skipped_label,
        play_is_skipped_flag
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id, play_is_skipped_label ORDER BY duration_sec DESC, id DESC, play_id DESC) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        release_date_label,
        duration_sec_bucket,
        play_id,
        play_is_skipped,
        play_listener_id,
        play_is_skipped_label,
        play_is_skipped_flag,
        FIRST_VALUE(play_played_at) IGNORE NULLS OVER (ORDER BY release_date, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_play_played_at,
        ROUND(AVG(duration_sec) OVER (PARTITION BY play_is_skipped_label, YEAR(release_date)), 2) AS play_is_skipped_label_release_date_year_avg_dura,
        DENSE_RANK() OVER w_all_by_duration_sec AS duration_sec_dense_rank
    FROM latest_per_key
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec ASC NULLS LAST, id, play_id)
)
-- Final projection
SELECT
    id,
    title,
    release_date,
    release_date_label,
    duration_sec_bucket,
    play_id,
    play_is_skipped,
    play_is_skipped_flag,
    first_known_play_played_at,
    play_is_skipped_label_release_date_year_avg_dura,
    ABS(duration_sec_dense_rank - 5) AS duration_sec_dense_rank_distance
FROM metrics
WHERE play_listener_id <= 11
ORDER BY duration_sec_bucket DESC NULLS FIRST

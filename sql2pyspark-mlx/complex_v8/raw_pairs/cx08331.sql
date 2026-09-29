WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        ABS(duration_sec - 213) AS duration_sec_distance
    FROM tracks
    WHERE (artist_id IS NOT NULL OR NOT ARRAY_CONTAINS(moods, 'sad'))
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_bucket,
        duration_sec_distance,
        MONTH(release_date) AS release_date_month,
        FLOOR(duration_sec_bucket / 50) * 50 AS duration_sec_bucket_bucket,
        COALESCE(bpm, 0) AS bpm_filled
    FROM valid_tracks
    WHERE bpm >= 82
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(bpm) OVER (PARTITION BY YEAR(release_date) ORDER BY release_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        LAST_VALUE(duration_sec_bucket_bucket) OVER (PARTITION BY YEAR(release_date) ORDER BY release_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec_bucket_bucket,
        MIN(release_date_month) OVER w_release_date_month_is_explic AS release_date_month_is_explicit_min_release_date_,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit) AS rows_in_release_date_month_is_explicit,
        FIRST_VALUE(duration_sec_distance) OVER (PARTITION BY YEAR(release_date) ORDER BY release_date_month ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_distance
    FROM enriched
    WINDOW w_release_date_month_is_explic AS (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit)
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.duration_sec_bucket,
        s.duration_sec_distance,
        s.release_date_month,
        s.duration_sec_bucket_bucket,
        s.bpm_filled,
        s.first_bpm,
        s.last_duration_sec_bucket_bucket,
        s.release_date_month_is_explicit_min_release_date_,
        s.rows_in_release_date_month_is_explicit,
        s.first_duration_sec_distance,
        t.mood_pos,
        t.mood
    FROM metrics s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(first_bpm) OVER (PARTITION BY is_explicit ORDER BY release_date_month DESC NULLS FIRST, id, mood_pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_bpm,
        RANK() OVER (PARTITION BY release_date_month, is_explicit ORDER BY duration_sec_bucket_bucket ASC, id, mood_pos) AS duration_sec_bucket_bucket_rank,
        LAG(release_date) OVER w_release_date_month_is_explic_2 AS prev_release_date,
        ROUND(duration_sec_bucket / NULLIF(SUM(duration_sec_bucket) OVER (PARTITION BY mood, is_explicit), 0), 4) AS duration_sec_bucket_share_of_mood_is_explicit,
        SUM(duration_sec_bucket_bucket) OVER (PARTITION BY release_date_month, is_explicit ORDER BY duration_sec_bucket_bucket ASC, id, mood_pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_bucket_bucket,
        FIRST_VALUE(release_date_month) IGNORE NULLS OVER (PARTITION BY release_date_month, is_explicit ORDER BY duration_sec_bucket_bucket ASC, id, mood_pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_month
    FROM per_label
    WINDOW w_release_date_month_is_explic_2 AS (PARTITION BY release_date_month, is_explicit ORDER BY duration_sec_bucket_bucket ASC, id, mood_pos)
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.release_date,
        s.duration_sec_bucket,
        s.duration_sec_distance,
        s.release_date_month,
        s.duration_sec_bucket_bucket,
        s.bpm_filled,
        s.first_bpm,
        s.last_duration_sec_bucket_bucket,
        s.rows_in_release_date_month_is_explicit,
        s.mood_pos,
        s.duration_sec_bucket_bucket_rank,
        s.prev_release_date,
        s.running_duration_sec_bucket_bucket,
        s.first_known_release_date_month,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.ms_played AS play_ms_played
    FROM sequenced s
    JOIN plays p ON p.track_id = s.id AND TO_DATE(p.played_at) < DATE '2025-03-02'
)
-- Final projection
SELECT
    id,
    duration_sec_bucket,
    duration_sec_distance,
    release_date_month,
    duration_sec_bucket_bucket,
    bpm_filled,
    first_bpm,
    last_duration_sec_bucket_bucket,
    rows_in_release_date_month_is_explicit,
    duration_sec_bucket_bucket_rank,
    prev_release_date,
    running_duration_sec_bucket_bucket,
    first_known_release_date_month,
    play_played_at,
    play_device,
    COALESCE(first_bpm, 0) AS first_bpm_filled,
    CONCAT(play_device, '#', CAST(duration_sec_distance AS STRING)) AS play_device_ref
FROM with_children_plays
ORDER BY duration_sec_bucket DESC NULLS FIRST, prev_release_date

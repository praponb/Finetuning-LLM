WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.duration_sec_bucket,
        t.mood
    FROM base s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date
    FROM flattened s
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY duration_sec_bucket NULLS LAST, id, mood) AS duration_sec_bucket_seq,
        RANK() OVER (PARTITION BY is_explicit ORDER BY last_track_release_date DESC, id, mood) AS last_track_release_date_rank,
        SUM(duration_sec_bucket) OVER (PARTITION BY mood, DATE_TRUNC('MONTH', last_track_release_date) ORDER BY duration_sec_bucket, id, mood ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_sum_4,
        RANK() OVER (ORDER BY duration_sec_bucket NULLS LAST, id, mood) AS duration_sec_bucket_rank
    FROM with_stats
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(last_track_release_date_rank) OVER (PARTITION BY duration_sec_bucket_rank) AS duration_sec_bucket_rank_max_last_track_release_,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY duration_sec_bucket_seq ORDER BY bpm DESC, id, mood ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY duration_sec_bucket_seq ORDER BY bpm DESC, id, mood ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        duration_sec_bucket - LAG(duration_sec_bucket) OVER w_duration_sec_bucket_seq_by_b_3 AS duration_sec_bucket_change,
        ROUND(duration_sec_bucket_rank - AVG(duration_sec_bucket_rank) OVER w_duration_sec_bucket_rank, 2) AS duration_sec_bucket_rank_vs_duration_sec_bucket_,
        DENSE_RANK() OVER w_duration_sec_bucket_seq_by_b_4 AS bpm_dense_rank
    FROM with_windows
    WINDOW w_duration_sec_bucket_seq_by_b_3 AS (PARTITION BY duration_sec_bucket_seq ORDER BY bpm DESC, id, mood), w_duration_sec_bucket_rank AS (PARTITION BY duration_sec_bucket_rank), w_duration_sec_bucket_seq_by_b_4 AS (PARTITION BY duration_sec_bucket_seq ORDER BY bpm DESC)
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.duration_sec_bucket,
        s.mood,
        s.duration_sec_bucket_seq,
        s.last_track_release_date_rank,
        s.duration_sec_bucket_sum_4,
        s.duration_sec_bucket_rank_vs_duration_sec_bucket_,
        s.bpm_dense_rank,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM ranked s
    JOIN plays p ON p.track_id = s.id
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id
        )
)
-- Final projection
SELECT
    id,
    bpm,
    duration_sec_bucket,
    mood,
    duration_sec_bucket_seq,
    duration_sec_bucket_sum_4,
    duration_sec_bucket_rank_vs_duration_sec_bucket_,
    bpm_dense_rank,
    play_id,
    play_ms_played,
    play_source,
    play_played_at,
    ABS(play_ms_played - 218129) AS play_ms_played_distance,
    INITCAP(TRIM(play_source)) AS play_source_clean
FROM with_activity
ORDER BY bpm_dense_rank, duration_sec_bucket_sum_4 DESC

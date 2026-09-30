WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        UPPER(TRIM(title)) AS title_clean,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-07-01' AND DATE '2025-11-29'
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.duration_sec_bucket,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM prepared s
    LEFT JOIN plays p ON p.track_id = s.id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        moods,
        duration_sec_bucket,
        play_id,
        play_source,
        play_played_at,
        ROUND(duration_sec_bucket - AVG(duration_sec_bucket) OVER w_play_id_play_device, 2) AS duration_sec_bucket_vs_play_id_play_device_avg,
        LAG(duration_sec_bucket) OVER (PARTITION BY play_source ORDER BY play_played_at DESC NULLS FIRST, id, play_id) AS prev_duration_sec_bucket,
        DENSE_RANK() OVER w_play_id_play_source_by_durat AS duration_sec_bucket_dense_rank,
        LEAD(play_played_at) OVER w_play_source_by_play_played_a AS next_play_played_at
    FROM with_children
    WINDOW w_play_id_play_device AS (PARTITION BY play_id, play_device), w_play_id_play_source_by_durat AS (PARTITION BY play_id, play_source ORDER BY duration_sec_bucket), w_play_source_by_play_played_a AS (PARTITION BY play_source ORDER BY play_played_at DESC NULLS FIRST, id, play_id)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2,
        p.last_played_at
    FROM with_windows s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
)
SELECT
    id,
    duration_sec,
    moods,
    duration_sec_bucket,
    play_id,
    play_played_at,
    duration_sec_bucket_vs_play_id_play_device_avg,
    duration_sec_bucket_dense_rank,
    next_play_played_at,
    play_count,
    play_count_2,
    last_played_at,
    CASE WHEN duration_sec >= 365 THEN 'A' WHEN duration_sec >= 164 THEN 'B' ELSE 'C' END AS duration_sec_tier,
    CASE WHEN prev_duration_sec_bucket >= 321 THEN 'A' WHEN prev_duration_sec_bucket >= 189 THEN 'B' ELSE 'C' END AS prev_duration_sec_bucket_tier
FROM qualified
WHERE play_id >= 99
ORDER BY id DESC

WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE listener_id <= 9
),
with_parent AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        t.moods AS track_moods,
        t.bpm AS track_bpm,
        a.artist_name,
        a.genre AS artist_genre
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY device ORDER BY track_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROUND(AVG(track_bpm) OVER (PARTITION BY device), 2) AS device_avg_track_bpm,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY track_bpm DESC, id) AS track_bpm_dense_rank
    FROM with_parent
),
unique_rows AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        track_moods,
        track_bpm,
        artist_name,
        artist_genre,
        last_known_played_at,
        device_avg_track_bpm,
        track_bpm_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY last_known_played_at DESC, id DESC) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM unique_rows s
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        track_bpm,
        artist_name,
        last_known_played_at,
        last_played_at,
        MAX(device_avg_track_bpm) OVER (PARTITION BY is_skipped, device) AS is_skipped_device_max_device_avg_track_bpm,
        COUNT(*) OVER w_is_skipped_device AS rows_in_is_skipped_device,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY is_skipped, device), 0), 4) AS track_bpm_share_of_is_skipped_device,
        FIRST_VALUE(device_avg_track_bpm) OVER (ORDER BY device_avg_track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_device_avg_track_bpm
    FROM with_counts
    WINDOW w_is_skipped_device AS (PARTITION BY is_skipped, device)
),
with_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.track_bpm,
        s.artist_name,
        s.last_known_played_at,
        s.last_played_at,
        s.is_skipped_device_max_device_avg_track_bpm,
        s.rows_in_is_skipped_device,
        s.first_device_avg_track_bpm,
        l.interests AS listener_interests,
        l.username AS listener_username,
        t.bpm AS track_bpm_2
    FROM sequenced s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    artist_name,
    last_known_played_at,
    last_played_at,
    is_skipped_device_max_device_avg_track_bpm,
    rows_in_is_skipped_device,
    track_bpm_2,
    CONCAT_WS(': ', artist_name, listener_username) AS artist_name_listener_username_label
FROM with_details
ORDER BY last_played_at DESC NULLS FIRST

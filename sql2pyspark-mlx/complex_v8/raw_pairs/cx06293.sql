WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE ms_played <= 308104
      AND played_at < TIMESTAMP '2025-02-12 11:00:00'
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = TRUE) FROM plays p WHERE p.listener_id = s.listener_id) AS play_is_skipped_count,
        (SELECT COUNT(CASE WHEN p.device NOT IN ('desktop', 'phone') THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_device_count
    FROM filtered_plays s
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        play_device_count,
        play_count,
        ROUND(STDDEV(play_is_skipped_count), 2) AS stddev_play_is_skipped_count,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS cnt,
        COUNT(DISTINCT device) AS distinct_devices
    FROM with_counts
    WHERE HOUR(played_at) BETWEEN 9 AND 11
    GROUP BY listener_id, play_device_count, play_count
),
with_details AS (
    -- Join listeners
    SELECT
        s.listener_id,
        s.play_device_count,
        s.play_count,
        s.stddev_play_is_skipped_count,
        s.last_played_at,
        s.cnt,
        l.signup_date AS listener_signup_date
    FROM summary s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    listener_id,
    play_device_count,
    play_count,
    last_played_at,
    listener_signup_date,
    DATEDIFF(DATE '2025-06-30', last_played_at) AS days_since_last_played_at,
    TO_DATE(last_played_at) AS last_played_at_date
FROM with_details
ORDER BY listener_id, last_played_at_date

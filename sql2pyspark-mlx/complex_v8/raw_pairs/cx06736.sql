WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ABS(duration_sec - 352) AS duration_sec_distance
    FROM tracks
    WHERE duration_sec >= 184
      AND is_explicit = TRUE
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.duration_sec_distance,
        p.id AS play_id,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM filtered s
    JOIN plays p ON p.track_id = s.id
),
filtered_2 AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = FALSE
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        duration_sec_distance,
        play_id,
        play_device,
        play_listener_id,
        ROUND(duration_sec_distance / NULLIF(SUM(duration_sec_distance) OVER w_play_device, 0), 4) AS duration_sec_distance_share_of_play_device,
        MAX(duration_sec) OVER (PARTITION BY artist_id, play_listener_id ORDER BY duration_sec_distance, id, play_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7,
        LEAD(release_date) OVER (PARTITION BY artist_id, play_listener_id ORDER BY duration_sec_distance, id, play_id) AS next_release_date,
        SUM(duration_sec) OVER (PARTITION BY play_device) AS play_device_sum_duration_sec
    FROM filtered_2
    WINDOW w_play_device AS (PARTITION BY play_device)
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.title,
        s.release_date,
        s.duration_sec_distance,
        s.play_id,
        s.play_device,
        s.play_listener_id,
        s.duration_sec_distance_share_of_play_device,
        s.duration_sec_max_7,
        s.next_release_date,
        s.play_device_sum_duration_sec,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date
    FROM with_windows s
    JOIN listeners l ON l.id = s.play_listener_id
    WHERE l.signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-10-18'
)
SELECT id, play_id, metric, amount
FROM (
    SELECT id, play_id, duration_sec_distance, listener_birth_year, play_device_sum_duration_sec
    FROM with_details_listener
) src
UNPIVOT (
    amount FOR metric IN (duration_sec_distance, listener_birth_year, play_device_sum_duration_sec)
) unp

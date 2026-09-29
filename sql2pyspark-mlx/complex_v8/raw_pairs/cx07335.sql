WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        UPPER(TRIM(device)) AS device_clean
    FROM plays
    WHERE source IN ('search', 'album')
      AND ms_played BETWEEN 52009 AND 324437
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.device_clean,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country,
        l.username AS listener_username,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM prepared_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, listener_country, track_duration_sec
    FROM with_parent
) src
PIVOT (
    MAX(track_duration_sec) AS max_v, SUM(track_duration_sec) AS sum_v
    FOR listener_country IN ('JP', 'KR', 'UK', 'TH')
)
ORDER BY track_id

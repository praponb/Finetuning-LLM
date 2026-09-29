WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE is_skipped = TRUE
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.*,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods
    FROM prepared_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        device_or_unknown,
        listener_plan,
        track_duration_sec,
        LAG(track_duration_sec) OVER w_all_by_track_duration_sec AS prev_track_duration_sec,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        SIZE(COLLECT_SET(source) OVER w_device) AS distinct_source_in_device,
        NTILE(10) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', listener_signup_date) ORDER BY track_duration_sec ASC, id) AS track_duration_sec_decile,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY track_id), 0), 4) AS track_duration_sec_share_of_track_id,
        LAG(listener_signup_date) OVER w_all_by_track_duration_sec AS prev_listener_signup_date,
        MAX(ms_played) OVER (PARTITION BY device) AS device_max_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', listener_signup_date) ORDER BY track_duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7
    FROM enriched_listener
    WINDOW w_all_by_track_duration_sec AS (ORDER BY track_duration_sec DESC, id), w_track_id AS (PARTITION BY track_id), w_device AS (PARTITION BY device)
)
SELECT
    id,
    track_id,
    device,
    source,
    device_or_unknown,
    listener_plan,
    track_duration_sec,
    prev_track_duration_sec,
    rows_in_track_id,
    track_duration_sec_share_of_track_id,
    prev_listener_signup_date,
    device_max_ms_played,
    ABS(track_duration_sec - 344) AS track_duration_sec_distance
FROM with_trends
ORDER BY device_or_unknown

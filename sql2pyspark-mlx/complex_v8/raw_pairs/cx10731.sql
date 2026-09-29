WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE device IS NOT NULL
      AND track_id >= 24
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source,
        s.device_len,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.moods AS track_moods
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        device_len,
        track_duration_sec,
        track_title,
        COUNT(*) OVER w_device_len_device AS rows_in_device_len_device,
        SUM(ms_played) OVER (PARTITION BY device_len) AS device_len_sum_ms_played,
        LAG(track_duration_sec, 2) OVER (ORDER BY device_len DESC, id) AS prev2_track_duration_sec,
        NTH_VALUE(track_duration_sec, 2) OVER (ORDER BY device_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec,
        ROUND(AVG(track_duration_sec) OVER (ORDER BY device_len NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS track_duration_sec_avg_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY device_len NULLS LAST), 3) AS device_len_percent_rank,
        ROUND(ms_played - AVG(ms_played) OVER w_device_len_device, 2) AS ms_played_vs_device_len_device_avg,
        device_len - LAG(device_len) OVER (ORDER BY device_len DESC, id) AS device_len_change,
        MAX(device_len) OVER (PARTITION BY device_len, device) AS device_len_device_max_device_len,
        NTH_VALUE(device_len, 2) OVER (ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_device_len,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER w_device_len_device, 0), 4) AS track_duration_sec_share_of_device_len_device
    FROM combined_details_track
    WINDOW w_device_len_device AS (PARTITION BY device_len, device)
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    device_len,
    track_duration_sec,
    rows_in_device_len_device,
    device_len_sum_ms_played,
    prev2_track_duration_sec,
    track_duration_sec_avg_3_prior,
    device_len_percent_rank,
    device_len_change,
    device_len_device_max_device_len,
    second_device_len,
    track_duration_sec_share_of_device_len_device,
    COALESCE(ms_played, 0) AS ms_played_filled,
    LEAST(device_len_change, second_device_len) AS least_device_len_change_second_device_len
FROM windowed
ORDER BY track_id DESC

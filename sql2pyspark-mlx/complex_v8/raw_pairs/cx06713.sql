WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE ms_played <= 333795
),
with_details AS (
    -- Join tracks
    SELECT
        s.*,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        source_or_unknown,
        track_duration_sec,
        track_moods,
        track_bpm,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY device ORDER BY ms_played DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY source_or_unknown, device), 0), 4) AS track_bpm_share_of_source_or_unknown_device,
        RANK() OVER w_device_len_played_at_year_by AS track_duration_sec_rank,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY track_id), 0), 4) AS track_bpm_share_of_track_id,
        COUNT(*) OVER (PARTITION BY device_len) AS rows_in_device_len,
        track_bpm - LAG(track_bpm) OVER (PARTITION BY source_or_unknown ORDER BY track_duration_sec ASC, id) AS track_bpm_change,
        ms_played - LAG(ms_played) OVER (PARTITION BY device_len, YEAR(played_at) ORDER BY track_duration_sec DESC NULLS FIRST, id) AS ms_played_change,
        ROUND(CUME_DIST() OVER (PARTITION BY source_or_unknown ORDER BY track_duration_sec ASC), 3) AS track_duration_sec_cume_dist,
        SUM(track_duration_sec) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec,
        MIN(track_bpm) OVER (PARTITION BY source_or_unknown, device) AS source_or_unknown_device_min_track_bpm,
        SUM(device_len) OVER (ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS device_len_sum_4,
        LAST_VALUE(track_bpm) OVER (PARTITION BY YEAR(played_at), is_skipped ORDER BY device_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm,
        SUM(device_len) OVER (PARTITION BY source_or_unknown, device) AS source_or_unknown_device_sum_device_len,
        LAG(device_len) OVER (PARTITION BY device ORDER BY ms_played DESC NULLS FIRST, id) AS prev_device_len,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY source_or_unknown, device), 0), 4) AS track_duration_sec_share_of_source_or_unknown_de
    FROM with_details
    WINDOW w_device_len_played_at_year_by AS (PARTITION BY device_len, YEAR(played_at) ORDER BY track_duration_sec DESC NULLS FIRST)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE track_duration_sec_rank <= 5
      AND source_or_unknown = 'playlist'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(track_duration_sec_cume_dist) OVER (ORDER BY rows_in_device_len DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_duration_sec_cume_dist_sum_3,
        COUNT(*) OVER (PARTITION BY is_skipped, prev_device_len) AS rows_in_is_skipped_prev_device_len,
        SUM(prev_device_len) OVER (PARTITION BY is_skipped, prev_device_len) AS is_skipped_prev_device_len_sum_prev_device_len,
        ROUND(rows_in_device_len / NULLIF(SUM(rows_in_device_len) OVER (PARTITION BY is_skipped, prev_device_len), 0), 4) AS rows_in_device_len_share_of_is_skipped_prev_devi,
        ROUND(AVG(running_track_duration_sec) OVER (ORDER BY track_duration_sec_cume_dist DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS running_track_duration_sec_avg_2_prior,
        LAG(last_track_bpm) OVER (ORDER BY rows_in_device_len DESC, id) AS prev_last_track_bpm,
        LAG(track_duration_sec_cume_dist, 2) OVER w_all_by_rows_in_device_len AS prev2_track_duration_sec_cume_dist
    FROM first_rows
    WINDOW w_all_by_rows_in_device_len AS (ORDER BY rows_in_device_len DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    source,
    source_or_unknown,
    track_duration_sec,
    track_moods,
    track_bpm,
    track_bpm_share_of_source_or_unknown_device,
    track_duration_sec_rank,
    track_bpm_share_of_track_id,
    rows_in_device_len,
    track_bpm_change,
    ms_played_change,
    running_track_duration_sec,
    source_or_unknown_device_min_track_bpm,
    device_len_sum_4,
    last_track_bpm,
    source_or_unknown_device_sum_device_len,
    prev_device_len,
    track_duration_sec_cume_dist_sum_3,
    rows_in_is_skipped_prev_device_len,
    is_skipped_prev_device_len_sum_prev_device_len,
    rows_in_device_len_share_of_is_skipped_prev_devi,
    running_track_duration_sec_avg_2_prior,
    prev_last_track_bpm,
    prev2_track_duration_sec_cume_dist,
    COALESCE(source_or_unknown, 'UNKNOWN') AS source_or_unknown_or_unknown
FROM windowed
ORDER BY track_id DESC NULLS FIRST, prev_device_len

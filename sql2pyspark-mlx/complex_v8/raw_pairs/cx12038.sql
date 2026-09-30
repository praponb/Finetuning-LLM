WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LOWER(TRIM(device)) AS device_clean,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE source IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        MIN(ms_played) OVER (PARTITION BY track_id) AS track_id_min_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at) ORDER BY played_at), 3) AS played_at_cume_dist,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        ROW_NUMBER() OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY ms_played ASC, id) AS seq_no,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        SUM(ms_played) OVER w_track_id AS track_id_sum_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_device, 0), 4) AS ms_played_share_of_device
    FROM valid
    WINDOW w_track_id AS (PARTITION BY track_id), w_device AS (PARTITION BY device)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rows_in_track_id) OVER (PARTITION BY device), 2) AS device_avg_rows_in_track_id,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        SUM(ms_played_share_of_device) OVER (PARTITION BY device) AS device_sum_ms_played_share_of_device,
        MAX(rows_in_track_id) OVER w_device AS device_max_rows_in_track_id
    FROM windowed
    WINDOW w_device AS (PARTITION BY device)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    device_clean,
    max_ms_played_so_far,
    rows_in_track_id,
    track_id_min_ms_played,
    ms_played_vs_device_avg,
    seq_no,
    ms_played_avg_2_prior,
    track_id_sum_ms_played,
    ms_played_share_of_device,
    device_avg_rows_in_track_id,
    device_sum_ms_played_share_of_device,
    device_max_rows_in_track_id,
    HOUR(played_at) AS played_at_hour,
    DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
FROM with_trends

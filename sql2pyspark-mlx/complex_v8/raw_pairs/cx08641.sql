WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_7d,
        s.prior_ms_played_7d,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM covered s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND p.played_at < TIMESTAMP '2025-02-24 12:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(play_source, '#', CAST(prior_ms_played_7d AS STRING)) AS play_source_ref,
        DATE_TRUNC('HOUR', play_played_at) AS play_played_at_hour
    FROM line_level
    WHERE played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
per_group AS (
    -- Group-level totals
    SELECT
        play_device,
        prior_rows_7d,
        ROUND(STDDEV(prior_rows_7d), 2) AS stddev_prior_rows_7d,
        ROUND(100 * COUNT(CASE WHEN play_device = 'car' THEN 1 END) / COUNT(*), 1) AS pct_play_device_car,
        COUNT(*) AS row_count,
        ROUND(SUM(prior_rows_7d) / NULLIF(SUM(prior_ms_played_7d), 0), 3) AS prior_rows_7d_to_prior_ms_played_7d_ratio,
        COUNT(DISTINCT play_track_id) AS distinct_play_track_ids
    FROM tagged
    GROUP BY 1, 2
)
SELECT
    play_device,
    prior_rows_7d,
    stddev_prior_rows_7d,
    row_count,
    distinct_play_track_ids,
    LENGTH(TRIM(play_device)) AS play_device_len,
    LENGTH(TRIM(play_device)) AS play_device_len_2
FROM per_group
ORDER BY row_count DESC NULLS FIRST, stddev_prior_rows_7d DESC

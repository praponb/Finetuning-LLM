WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.listener_id) AS avg_play_ms_played,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS total_play_ms_played
    FROM filtered s
),
with_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.avg_play_ms_played,
        s.total_play_ms_played,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id,
        a.country AS artist_country
    FROM with_stats s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
    WHERE t.release_date BETWEEN DATE '2024-07-01' AND DATE '2025-08-02'
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(device, '#', CAST(track_artist_id AS STRING)) AS device_ref,
        CONCAT_WS(' / ', source, device) AS source_device_label
    FROM with_details
    WHERE track_release_date BETWEEN DATE '2024-05-12' AND DATE '2025-08-02'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(total_play_ms_played - AVG(total_play_ms_played) OVER w_device, 2) AS total_play_ms_played_vs_device_avg,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        SUM(avg_play_ms_played) OVER (PARTITION BY source ORDER BY total_play_ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_play_ms_played,
        DENSE_RANK() OVER (PARTITION BY source ORDER BY total_play_ms_played NULLS LAST, id) AS total_play_ms_played_dense_rank,
        ROUND(AVG(avg_play_ms_played) OVER (PARTITION BY source ORDER BY total_play_ms_played NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS avg_play_ms_played_avg_4,
        ROUND(avg_play_ms_played / NULLIF(SUM(avg_play_ms_played) OVER (PARTITION BY device), 0), 4) AS avg_play_ms_played_share_of_device
    FROM derived
    WINDOW w_device AS (PARTITION BY device)
)
SELECT
    id,
    track_id,
    device,
    is_skipped,
    source,
    played_at_label,
    avg_play_ms_played,
    track_release_date,
    device_ref,
    source_device_label,
    total_play_ms_played_vs_device_avg,
    rows_in_device,
    running_avg_play_ms_played,
    total_play_ms_played_dense_rank,
    avg_play_ms_played_avg_4,
    avg_play_ms_played_share_of_device
FROM windowed
WHERE running_avg_play_ms_played >= 1203500.0
ORDER BY source_device_label DESC, avg_play_ms_played NULLS LAST, id
LIMIT 5

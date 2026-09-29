WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE source = 'album'
      AND HOUR(played_at) BETWEEN 9 AND 13
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at < TIMESTAMP '2025-02-18 12:00:00'
              AND p.track_id <= 16
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        MIN(ms_played) OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        COUNT(*) OVER w_device_listener_id AS rows_in_device_listener_id,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY is_skipped ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SUM(ms_played) OVER (PARTITION BY device, listener_id) AS device_listener_id_sum_ms_played,
        LAG(ms_played, 1, 0) OVER w_is_skipped_by_played_at AS prev_ms_played
    FROM filtered
    WINDOW w_device_listener_id AS (PARTITION BY device, listener_id), w_is_skipped_by_played_at AS (PARTITION BY is_skipped ORDER BY played_at ASC, id)
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source <> 'search'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        min_ms_played_so_far,
        rows_in_device_listener_id,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, device ORDER BY second_ms_played DESC), 3) AS second_ms_played_cume_dist,
        ROUND(AVG(second_ms_played) OVER (PARTITION BY track_id, device ORDER BY second_ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS second_ms_played_avg_7,
        NTILE(5) OVER w_track_id_device_by_second_ms_3 AS second_ms_played_quintile
    FROM qualified
    WINDOW w_track_id_device_by_second_ms_3 AS (PARTITION BY track_id, device ORDER BY second_ms_played DESC, id)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played
    FROM metrics s
),
tagged AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM profiled
    WHERE ms_played > 52009
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    min_ms_played_so_far,
    rows_in_device_listener_id,
    second_ms_played_cume_dist,
    second_ms_played_avg_7,
    min_play_ms_played,
    device_prefix,
    CASE WHEN play_count >= 6 THEN 'large' WHEN play_count >= 2 THEN 'medium' ELSE 'small' END AS play_count_tier
FROM tagged
WHERE play_count >= 2
ORDER BY id DESC NULLS FIRST, second_ms_played_avg_7 DESC NULLS FIRST
LIMIT 20

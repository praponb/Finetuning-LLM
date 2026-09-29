WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE device IN ('tv', 'desktop')
      AND track_id >= 2
),
unique_rows AS (
    -- Keep the latest row per track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM filtered_plays
    ) ranked
    WHERE row_num = 1
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS min_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT COUNT(*) FILTER (WHERE p.device IN ('speaker', 'car')) FROM plays p WHERE p.listener_id = s.listener_id) AS play_device_count
    FROM unique_rows s
),
listener_id_stats AS (
    -- Max play_device_count per listener_id
    SELECT listener_id, MAX(play_device_count) AS max_play_device_count
    FROM summarized_rows
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_play_device_count,
        ROUND(s.play_device_count - g.max_play_device_count, 2) AS play_device_count_minus_max
    FROM summarized_rows s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.play_device_count = g.max_play_device_count
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        play_count,
        play_device_count,
        max_play_device_count,
        play_device_count_minus_max,
        CASE WHEN max_play_device_count >= 4.4 THEN 'A' WHEN max_play_device_count >= 2.0 THEN 'B' ELSE 'C' END AS max_play_device_count_tier,
        TO_DATE(played_at) AS played_at_date,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m
    FROM outliers
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    play_device_count,
    max_play_device_count,
    max_play_device_count_tier,
    played_at_date,
    played_at_plus_30m
FROM enriched

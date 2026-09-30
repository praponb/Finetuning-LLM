WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE ms_played BETWEEN 73335 AND 245815
      AND is_skipped = TRUE
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(days_since_played_at - 385) AS days_since_played_at_distance,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        HOUR(played_at) AS played_at_hour
    FROM scoped
    WHERE days_since_played_at BETWEEN 140 AND 525
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device = 'desktop'
              AND p.source IN ('playlist', 'radio')
        )
      AND s.track_id IS NOT NULL
),
listener_id_stats AS (
    -- Median days_since_played_at per listener_id
    SELECT listener_id, PERCENTILE_APPROX(days_since_played_at, 0.5) AS median_days_since_played_at
    FROM filtered
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_played_at,
        ROUND(s.days_since_played_at - g.median_days_since_played_at, 2) AS days_since_played_at_minus_median
    FROM filtered s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.days_since_played_at >= g.median_days_since_played_at
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.days_since_played_at,
        s.days_since_played_at_distance,
        s.device_or_unknown,
        s.played_at_hour,
        s.days_since_played_at_minus_median,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM above_average s
    JOIN plays p ON p.listener_id = s.listener_id
),
filtered_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id >= 19
              AND p.is_skipped = FALSE
        )
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    days_since_played_at,
    days_since_played_at_distance,
    days_since_played_at_minus_median,
    play_id,
    play_is_skipped,
    LENGTH(TRIM(device_or_unknown)) AS device_or_unknown_len
FROM filtered_2
ORDER BY device DESC, id NULLS LAST

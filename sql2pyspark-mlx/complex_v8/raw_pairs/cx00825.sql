WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped
    FROM plays
    WHERE is_skipped = TRUE
      AND listener_id IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped,
        HOUR(played_at) AS played_at_hour,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        CASE WHEN ms_played >= 221269 THEN 'HIGH' WHEN ms_played >= 120663 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier
    FROM prepared_plays
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.played_at_year,
        s.ms_played_tier,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.listener_id AS play_listener_id
    FROM labeled s
    JOIN plays p ON p.track_id = s.track_id AND p.listener_id IS NOT NULL
),
labeled_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', ms_played_tier, play_device) AS ms_played_tier_play_device_label
    FROM detail_rows
    WHERE play_listener_id >= 8
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        COUNT(*) AS play_count,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count,
        p.last_played_at
    FROM labeled_2 s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN avg_play_ms_played >= 321000.0 THEN 'large' WHEN avg_play_ms_played >= 84000.0 THEN 'medium' ELSE 'small' END AS avg_play_ms_played_tier,
        CONCAT(play_device, '#', CAST(play_id AS STRING)) AS play_device_ref,
        COALESCE(play_count, 0) AS play_count_filled
    FROM summarized_rows
    WHERE play_id <= 68
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.track_id,
        s.ms_played_tier,
        s.play_id,
        s.play_device,
        s.play_source,
        s.play_listener_id,
        s.ms_played_tier_play_device_label,
        s.last_played_at,
        s.avg_play_ms_played_tier,
        p.id AS play_id_2,
        p.listener_id AS play_listener_id_2,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM scored s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT
    id,
    track_id,
    play_source,
    play_listener_id,
    ms_played_tier_play_device_label,
    last_played_at,
    avg_play_ms_played_tier,
    play_id_2,
    play_ms_played,
    LENGTH(TRIM(play_device)) AS play_device_len,
    SUBSTRING(TRIM(play_source), 1, 4) AS play_source_prefix
FROM with_children
ORDER BY play_id_2, avg_play_ms_played_tier DESC

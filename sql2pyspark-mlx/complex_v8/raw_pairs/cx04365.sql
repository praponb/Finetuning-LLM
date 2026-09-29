WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        MONTH(played_at) AS played_at_month,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        source,
        played_at_month,
        played_at_label,
        device_slug,
        GREATEST(ms_played, played_at_month) AS greatest_ms_played_played_at_month
    FROM filtered
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played
    FROM annotated s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.played_at_month,
        s.played_at_label,
        s.device_slug,
        s.greatest_ms_played_played_at_month,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM summarized_rows s
    JOIN plays p ON p.listener_id = s.listener_id
)
-- Subtotals with GROUPING SETS
SELECT
    source,
    listener_id,
    played_at_month,
    SUM(CASE WHEN source = 'playlist' THEN ms_played ELSE 0 END) AS ms_played_source_playlist,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list,
    ROUND(SUM(ms_played) / NULLIF(SUM(played_at_month), 0), 3) AS ms_played_to_played_at_month_ratio
FROM exploded_detail
GROUP BY GROUPING SETS ((source, listener_id, played_at_month), (source), (listener_id))
ORDER BY source NULLS LAST, listener_id NULLS LAST, played_at_month NULLS LAST

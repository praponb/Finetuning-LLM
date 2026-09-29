WITH prepared AS (
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
        INITCAP(TRIM(source)) AS source_clean,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE listener_id IS NOT NULL
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source_clean,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.track_id AS play_track_id
    FROM prepared s
    JOIN plays p ON p.listener_id = s.listener_id
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.play_id,
        s.play_source,
        s.play_device,
        s.play_track_id,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM exploded_detail_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT COUNT(*) FILTER (WHERE p.device IS NOT NULL) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM enriched_listener s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, ms_played, play_device_count, play_count, listener_birth_year
    FROM profiled
) src
UNPIVOT (
    reading FOR metric_name IN (ms_played, play_device_count, play_count, listener_birth_year)
) unp

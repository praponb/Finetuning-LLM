WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE (ms_played >= 168782 OR source = 'playlist')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        LAST_VALUE(ms_played) OVER (PARTITION BY listener_id, MONTH(played_at) ORDER BY ms_played NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY source, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device, source)) AS distinct_track_id_in_device_source,
        ROW_NUMBER() OVER (PARTITION BY source, listener_id ORDER BY ms_played, id) AS rn,
        MAX(ms_played) OVER (PARTITION BY device, source) AS device_source_max_ms_played
    FROM base_plays
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(ms_played) AS min_play_ms_played,
        MIN(played_at) AS first_played_at,
        COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played,
        p.first_played_at,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM metrics s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source,
        s.days_since_played_at,
        s.source_slug,
        s.last_ms_played,
        s.first_known_played_at,
        s.distinct_track_id_in_device_source,
        s.rn,
        s.device_source_max_ms_played,
        s.min_play_ms_played,
        s.first_played_at,
        s.play_is_skipped_count,
        l.country AS listener_country,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM with_counts s
    JOIN listeners l ON l.id = s.listener_id AND l.signup_date BETWEEN DATE '2024-08-02' AND DATE '2025-10-18'
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source_slug,
        s.first_known_played_at,
        s.distinct_track_id_in_device_source,
        s.min_play_ms_played,
        s.first_played_at,
        s.listener_plan,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM enriched_listener s
    JOIN plays p ON p.track_id = s.track_id AND p.device IN ('phone', 'car')
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.first_known_played_at,
        s.first_played_at,
        s.play_id,
        s.play_is_skipped,
        s.play_played_at,
        p.id AS play_id_2,
        p.played_at AS play_played_at_2,
        p.ms_played AS play_ms_played
    FROM with_children s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT *
FROM (
    SELECT play_played_at, play_played_at_2, device, play_ms_played
    FROM with_children_plays
) src
PIVOT (
    SUM(play_ms_played)
    FOR device IN ('phone', 'desktop', 'car')
)
ORDER BY play_played_at, play_played_at_2

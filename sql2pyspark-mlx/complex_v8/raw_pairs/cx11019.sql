WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE listener_id >= 8
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        COUNT(CASE WHEN device = 'speaker' THEN 1 END) AS play_device_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM scoped s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source_slug,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM with_counts s
    LEFT JOIN plays p ON p.listener_id = s.listener_id AND p.is_skipped = TRUE
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN device IN ('car', 'tv') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group,
        DATEDIFF(DATE '2026-01-01', play_played_at) AS days_since_play_played_at,
        CONCAT(source_slug, '#', CAST(listener_id AS STRING)) AS source_slug_ref
    FROM exploded_detail_plays
),
unique_rows AS (
    -- Keep the latest row per play_source
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source_slug,
        play_id,
        play_track_id,
        play_played_at,
        play_is_skipped,
        play_source,
        device_group,
        days_since_play_played_at,
        source_slug_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_source ORDER BY play_played_at DESC, id DESC, play_id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    listener_id,
    is_skipped,
    source_slug,
    play_id,
    play_track_id,
    play_played_at,
    play_is_skipped,
    play_source,
    days_since_play_played_at,
    source_slug_ref,
    SUBSTRING(TRIM(device_group), 1, 3) AS device_group_prefix,
    LOWER(TRIM(source_slug)) AS source_slug_clean
FROM unique_rows
WHERE device_group <> 'other'

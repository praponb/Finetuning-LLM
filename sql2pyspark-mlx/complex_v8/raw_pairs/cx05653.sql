WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN device IN ('desktop', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE is_skipped = FALSE
      AND ms_played <= 287769
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM valid s
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(source_slug), ' ', '_')) AS source_slug_slug,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM summarized_rows
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.device_slug,
        s.device_prefix,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM annotated s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT *
FROM (
    SELECT listener_id, play_source, ms_played
    FROM detail_rows
) src
PIVOT (
    SUM(ms_played)
    FOR play_source IN ('search', 'album', 'playlist', 'radio')
)
ORDER BY listener_id

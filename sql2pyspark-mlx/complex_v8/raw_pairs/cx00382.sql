WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE is_skipped = FALSE
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.days_since_played_at,
        t.title AS track_title
    FROM cleaned_plays s
    JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    days_since_played_at,
    track_title,
    SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown
FROM enriched
WHERE is_skipped = TRUE
ORDER BY id DESC, source_prefix

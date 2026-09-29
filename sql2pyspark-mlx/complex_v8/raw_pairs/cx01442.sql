WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device NOT IN ('desktop', 'phone')
      AND listener_id <= 14
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = FALSE
        )
),
enriched AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source,
        l.interests AS listener_interests,
        l.username AS listener_username,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM filtered s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
unique_rows AS (
    -- Keep the latest row per source, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        source,
        listener_interests,
        listener_username,
        track_title,
        track_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, listener_id ORDER BY played_at DESC, id DESC) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, listener_id, source, track_duration_sec
    FROM unique_rows
) src
PIVOT (
    MIN(track_duration_sec)
    FOR source IN ('radio', 'album')
)

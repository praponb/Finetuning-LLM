WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        YEAR(played_at) AS played_at_year,
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE device <> 'phone'
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM base s
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM summarized_rows s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.device IN ('tv', 'desktop', 'car')
        )
      AND s.source <> 'playlist'
),
with_activity_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
      AND s.played_at >= TIMESTAMP '2025-01-19 13:00:00'
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    device_group,
    source_ref,
    play_count,
    DATE_TRUNC('WEEK', played_at) AS played_at_week,
    DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
FROM with_activity_2

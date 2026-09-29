WITH filtered_plays AS (
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
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE MONTH(played_at) IN (1, 3)
),
deduped AS (
    -- Keep the latest row per device, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_or_unknown,
        source_or_unknown,
        played_at_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM filtered_plays
    ) ranked
    WHERE rn = 1
),
source_stats AS (
    -- Max ms_played per source
    SELECT source, MAX(ms_played) AS max_ms_played
    FROM deduped
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM deduped s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played = g.max_ms_played
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    COALESCE(device, 'ALL') AS device,
    is_skipped,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    ROUND(STDDEV(ms_played_minus_max), 2) AS stddev_ms_played_minus_max
FROM above_average
GROUP BY GROUPING SETS ((listener_id, device, is_skipped), (listener_id))
ORDER BY listener_id NULLS LAST, device NULLS LAST, is_skipped NULLS LAST

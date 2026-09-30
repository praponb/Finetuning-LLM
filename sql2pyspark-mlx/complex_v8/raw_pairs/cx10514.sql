WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        CONCAT_WS('|', device, source) AS device_source_label
    FROM plays
    WHERE track_id >= 23
      AND is_skipped = TRUE
),
combined_details_track AS (
    -- Attach track attributes
    SELECT s.*, t.is_explicit AS track_is_explicit
    FROM recent_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN ms_played >= 239955 THEN 'large' WHEN ms_played >= 95960 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM combined_details_track
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM derived
    GROUP BY track_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM derived s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
)
SELECT *
FROM (
    SELECT listener_id, ms_played_tier, source, avg_ms_played
    FROM vs_group
) src
PIVOT (
    AVG(avg_ms_played)
    FOR source IN ('playlist', 'radio', 'album', 'search')
)
ORDER BY listener_id, ms_played_tier

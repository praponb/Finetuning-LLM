WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.birth_year AS listener_birth_year,
        t.duration_sec AS track_duration_sec
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    COALESCE(device, 'ALL') AS device,
    is_skipped,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM enriched
GROUP BY GROUPING SETS ((device, is_skipped), (device), (is_skipped), ())
ORDER BY device NULLS LAST, is_skipped NULLS LAST

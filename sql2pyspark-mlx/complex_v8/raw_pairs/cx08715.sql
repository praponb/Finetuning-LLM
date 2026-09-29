WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_TRUNC('DAY', played_at) AS played_at_day,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE NOT is_skipped
      AND played_at >= TIMESTAMP '2025-01-13 14:00:00'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        ms_played,
        device,
        played_at_day,
        source_group,
        CASE WHEN ms_played >= 245877 THEN 'large' WHEN ms_played >= 97825 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        played_at_day + INTERVAL 2 HOURS AS played_at_day_plus_2h,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM scoped_plays
    WHERE source <> 'radio'
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
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at_day_plus_2h, played_at_day, device, ms_played_filled
    FROM vs_group
) src
PIVOT (
    SUM(ms_played_filled)
    FOR device IN ('tv', 'desktop')
)
ORDER BY played_at_day_plus_2h, played_at_day

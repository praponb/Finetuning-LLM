WITH cleaned AS (
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
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CONCAT_WS('-', device, source) AS device_source_label
    FROM plays
    WHERE listener_id <= 19
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.device_source_label,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit,
        l.plan AS listener_plan
    FROM cleaned s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', track_release_date) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_plan), 2) AS ms_played_vs_listener_plan_avg
    FROM joined_track
),
labeled AS (
    -- Derive helper columns
    SELECT *, COALESCE(listener_plan, 'UNKNOWN') AS listener_plan_or_unknown
    FROM with_windows
    WHERE YEAR(track_release_date) = 2024
)
-- Subtotals with CUBE
SELECT
    track_id,
    COALESCE(listener_plan, 'ALL') AS listener_plan,
    GROUPING_ID() AS grouping_level,
    SUM(running_ms_played) AS total_running_ms_played,
    COUNT(*) AS cnt,
    ROUND(STDDEV(ms_played), 2) AS stddev_ms_played
FROM labeled
GROUP BY CUBE(track_id, listener_plan)

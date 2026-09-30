WITH scoped AS (
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
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
    WHERE listener_id IS NOT NULL
      AND is_skipped = TRUE
),
per_group AS (
    -- Group-level totals
    SELECT
        is_skipped,
        YEAR(played_at) AS played_at_year,
        track_id,
        SUM(ms_played) AS total_ms_played,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        ROUND(AVG(ms_played), 2) AS avg_ms_played,
        COUNT(*) AS cnt
    FROM scoped
    GROUP BY is_skipped, YEAR(played_at), track_id
),
annotated AS (
    -- Label and score rows
    SELECT
        is_skipped,
        played_at_year,
        track_id,
        total_ms_played,
        stddev_ms_played,
        avg_ms_played,
        avg_ms_played - played_at_year AS avg_ms_played_minus_played_at_year,
        ROUND(stddev_ms_played / NULLIF(cnt, 0), 3) AS stddev_ms_played_per_cnt
    FROM per_group
    WHERE played_at_year BETWEEN 2024 AND 2025
)
SELECT
    is_skipped,
    track_id,
    avg_ms_played,
    avg_ms_played_minus_played_at_year,
    stddev_ms_played_per_cnt
FROM annotated
ORDER BY stddev_ms_played_per_cnt, avg_ms_played

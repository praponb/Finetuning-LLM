WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        QUARTER(played_at) AS played_at_quarter
    FROM plays
    WHERE ms_played BETWEEN 49807 AND 245815
),
per_group AS (
    -- Summarise per group
    SELECT
        listener_id,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(SUM(ms_played) / NULLIF(SUM(played_at_quarter), 0), 3) AS ms_played_to_played_at_quarter_ratio,
        SUM(ms_played) AS total_ms_played,
        SUM(CASE WHEN source = 'search' THEN played_at_quarter ELSE 0 END) AS played_at_quarter_source_search
    FROM valid
    GROUP BY 1, 2
    HAVING MAX(ms_played) > 130888
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2,
        COUNT(*) FILTER (WHERE device IN ('speaker', 'car')) AS play_device_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM per_group s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
SELECT
    listener_id,
    source_list,
    ms_played_to_played_at_quarter_ratio,
    played_at_quarter_source_search,
    play_count,
    play_count_2,
    play_device_count,
    UPPER(TRIM(played_at_ym)) AS played_at_ym_clean
FROM with_counts
ORDER BY play_count_2 DESC

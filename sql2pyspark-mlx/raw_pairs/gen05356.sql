WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        CONCAT(page, ' ', event_type) AS page_event_type,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE page IS NOT DISTINCT FROM '/cart'
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        page_event_type,
        page_filled,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    island_id,
    MIN(event_time) AS first_seen,
    COUNT(*) AS row_count,
    MIN(event_time) AS started_at
FROM islands
GROUP BY GROUPING SETS ((CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, island_id), (CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY island_id_tier DESC NULLS FIRST;

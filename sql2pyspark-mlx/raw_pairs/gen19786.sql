WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        page,
        event_time,
        SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    event_type,
    AVG(session_id_2) AS avg_session_id_2,
    COUNT(DISTINCT page) AS distinct_pages,
    MAX(session_id_2) AS max_session_id_2,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', event_time) ORDER BY SIZE(COLLECT_SET(event_type)) DESC) AS num_event_types_percent_rank
FROM sessionized
GROUP BY 1, 2
ORDER BY search_rows ASC NULLS FIRST

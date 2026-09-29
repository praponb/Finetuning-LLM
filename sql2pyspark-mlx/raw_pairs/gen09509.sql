WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(page)) AS page_title,
        CONCAT(page, ' - ', event_type) AS page_event_type,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE page IS DISTINCT FROM '/home'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
deduped_web_events AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY duration_sec NULLS FIRST, id) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM deduped_web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    page_title,
    page_event_type,
    resolved_page,
    dedup_rank
FROM islands
ORDER BY duration_sec DESC;

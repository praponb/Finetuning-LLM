WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE event_time > TO_DATE('2023-10-11')
      AND page IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    COUNT(duration_sec) OVER w_running AS last4_count_duration_sec,
    MAX(duration_sec) OVER w_running AS last4_max_duration_sec,
    SUM(duration_sec) OVER w_running AS last4_sum_duration_sec,
    MIN(duration_sec) OVER w_running AS last4_min_duration_sec,
    MAX(duration_sec) OVER w_grp AS page_max_duration_sec
FROM valid
WINDOW w_running AS (PARTITION BY page, session_id ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY page);

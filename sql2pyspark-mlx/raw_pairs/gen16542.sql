WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CEIL(duration_sec) AS duration_sec_whole
    FROM web_events
    WHERE page IS NOT NULL
      AND page IN ('/checkout', '/home', '/search')
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY duration_sec_whole DESC, id) AS row_num
    FROM cleaned
) x
WHERE row_num = 1

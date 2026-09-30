WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        CONCAT_WS('-', event_type, page) AS event_type_page_key,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time > TO_DATE('2023-07-15')
      AND page IS NOT NULL
)
SELECT event_type
FROM valid
INTERSECT ALL
SELECT event_type
FROM web_events
WHERE event_type = 'add_to_cart'

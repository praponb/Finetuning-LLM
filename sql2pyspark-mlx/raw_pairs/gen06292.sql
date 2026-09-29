WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) / 365 AS years_since_event_time,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2023-09-13 22:00:00')
      AND page IS NOT NULL
)
SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM cleaned
) src
PIVOT (
    MAX(duration_sec) AS amount,
    COUNT(1) AS item_count
    FOR page IN ('/cart' AS cart, '/search' AS search)
)

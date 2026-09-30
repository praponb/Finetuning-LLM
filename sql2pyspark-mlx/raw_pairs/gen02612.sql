SELECT *
FROM (
    SELECT
        event_type,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    MAX(duration_sec) AS total,
    COUNT(1) AS item_count
    FOR page IN ('/checkout' AS checkout, '/cart' AS cart, '/home' AS home)
)

SELECT *
FROM (
    SELECT
        event_type,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    SUM(duration_sec)
    FOR page IN ('/checkout' AS checkout, '/product' AS product)
);

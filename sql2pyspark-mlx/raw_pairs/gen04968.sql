SELECT *
FROM (
    SELECT
        page,
        session_id,
        event_type,
        duration_sec
    FROM web_events
) src
PIVOT (
    SUM(duration_sec) AS value_sum,
    COUNT(1) AS cnt
    FOR event_type IN ('page_view', 'purchase')
)

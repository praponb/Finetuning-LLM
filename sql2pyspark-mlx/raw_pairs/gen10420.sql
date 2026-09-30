SELECT *
FROM (
    SELECT
        session_id,
        event_type,
        duration_sec
    FROM web_events
) src
PIVOT (
    SUM(duration_sec) AS value_sum
    FOR event_type IN ('click' AS click, 'purchase' AS purchase)
);

SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec,
    COUNT_IF(event_type = 'page_view') AS page_view_events,
    COUNT(DISTINCT id) AS distinct_ids
FROM web_events
GROUP BY 1
ORDER BY distinct_ids DESC NULLS LAST;

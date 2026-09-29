SELECT YEAR(event_time) event_year, SORT_ARRAY(COLLECT_SET(page)) page_list, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) search_duration_sec
FROM web_events
GROUP BY event_year
ORDER BY search_duration_sec;

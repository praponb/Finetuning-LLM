SELECT TO_DATE(event_time) event_day, SORT_ARRAY(COLLECT_SET(page)) page_list, COUNT(*) web_event_count FROM web_events WHERE session_id IN (1, 16, 37) GROUP BY TO_DATE(event_time)

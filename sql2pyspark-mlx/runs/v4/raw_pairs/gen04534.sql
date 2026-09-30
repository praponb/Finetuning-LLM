SELECT browser, country, SORT_ARRAY(COLLECT_SET(browser)) AS browser_list, COUNT(*) AS event_count, MAX(started_at) AS ended_at FROM sessions GROUP BY 1, 2 ORDER BY event_count NULLS FIRST;

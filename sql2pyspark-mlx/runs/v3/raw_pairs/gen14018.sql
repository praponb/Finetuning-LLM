SELECT CASE WHEN s.browser IN ('Chrome', 'Safari') THEN 'group_a' ELSE 'other' END browser_group, MIN(we.duration_sec) min_duration_sec, SORT_ARRAY(COLLECT_SET(we.event_type)) event_type_list
FROM web_events we
JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE we.event_type != 'search'
GROUP BY CASE WHEN s.browser IN ('Chrome', 'Safari') THEN 'group_a' ELSE 'other' END
ORDER BY browser_group
LIMIT 50;

SELECT c.is_active, MAX(s.started_at) last_started_at, SORT_ARRAY(COLLECT_SET(s.browser)) browser_list FROM sessions s LEFT OUTER JOIN customers c ON s.customer_id = c.id GROUP BY c.is_active

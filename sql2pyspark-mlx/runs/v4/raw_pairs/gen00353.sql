SELECT c.segment, SORT_ARRAY(COLLECT_SET(s.device)) AS device_list FROM sessions s INNER JOIN customers c ON c.id = s.customer_id GROUP BY c.segment ORDER BY segment DESC LIMIT 50;

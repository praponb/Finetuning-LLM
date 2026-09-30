SELECT session_id, SORT_ARRAY(COLLECT_LIST(page)) AS pages FROM web_events GROUP BY session_id;

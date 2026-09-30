SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) page_list FROM web_events WHERE event_type IN ('add_to_cart', 'search', 'purchase') OR page = '/checkout' GROUP BY session_id

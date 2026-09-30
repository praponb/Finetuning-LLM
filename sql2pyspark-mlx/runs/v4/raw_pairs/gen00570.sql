SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) page_values, SORT_ARRAY(COLLECT_SET(page)) page_list FROM web_events

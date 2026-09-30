SELECT SORT_ARRAY(COLLECT_SET(segment)) AS segment_list FROM customers WHERE (MONTH(signup_date) IN (3, 6, 11) AND country <> 'DE') OR age BETWEEN 37 AND 45

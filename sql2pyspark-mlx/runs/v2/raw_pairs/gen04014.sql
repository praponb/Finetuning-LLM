SELECT c.segment, SORT_ARRAY(COLLECT_SET(o.status)) AS status_list FROM orders o LEFT OUTER JOIN customers c ON o.customer_id = c.id GROUP BY c.segment ORDER BY c.segment DESC LIMIT 5;

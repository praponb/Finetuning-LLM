SELECT c.segment, COUNT(*) cnt, SORT_ARRAY(COLLECT_SET(s.browser)) browser_list FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE UPPER(c.name) LIKE '%IA %' GROUP BY c.segment

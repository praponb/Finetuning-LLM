SELECT method, SORT_ARRAY(COLLECT_SET(method)) AS method_list, MAX(amount) AS max_amount FROM payments GROUP BY method;

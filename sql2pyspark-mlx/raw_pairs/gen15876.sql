SELECT COUNT(*) AS cnt, SUM(credit_limit) AS total_credit_limit, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values FROM customers;

SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values, ROUND(AVG(amount), 2) avg_amount FROM payments;

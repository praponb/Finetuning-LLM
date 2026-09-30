SELECT MAX(amount) AS max_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, ROUND(AVG(amount), 2) AS avg_amount FROM payments

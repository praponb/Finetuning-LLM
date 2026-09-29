SELECT segment, MIN(credit_limit) min_credit_limit, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) segment_values
FROM customers
WHERE is_active = TRUE
GROUP BY 1

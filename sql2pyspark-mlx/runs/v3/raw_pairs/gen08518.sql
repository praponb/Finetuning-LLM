SELECT method, ROUND(AVG(amount), 2) AS avg_amount, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM payments GROUP BY method ORDER BY avg_amount, method LIMIT 10;

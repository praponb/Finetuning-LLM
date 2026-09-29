SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, MIN(end_date) AS first_end_date
FROM subscriptions
WHERE YEAR(end_date) >= 2024 AND MONTH(start_date) IN (2, 5, 12) AND MONTH(start_date) IN (8, 11, 12)
ORDER BY first_end_date ASC, status_values ASC;

SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, COUNT(*) AS cnt
FROM shipments
WHERE shipped_date BETWEEN '2023-03-20' AND '2024-09-01' AND cost <= 28;

SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) AS status_values
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.opened_date)
ORDER BY 2 ASC;

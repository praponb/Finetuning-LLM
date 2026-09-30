SELECT YEAR(order_date) order_year, SORT_ARRAY(COLLECT_SET(status)) status_list, ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_store
FROM orders
WHERE MONTH(order_date) IN (3, 4, 5) OR channel IN ('partner', 'store')
GROUP BY YEAR(order_date)
HAVING ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 27;

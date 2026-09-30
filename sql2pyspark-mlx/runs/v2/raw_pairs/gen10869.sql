SELECT YEAR(delivered_date) AS delivered_year, SORT_ARRAY(COLLECT_SET(status)) AS status_list, MAX(shipped_date) AS last_shipped_date
FROM shipments
WHERE MONTH(delivered_date) IN (5, 7, 10) AND order_id IN (19, 21, 24, 37) AND status IN ('IN_TRANSIT', 'DELIVERED')
GROUP BY delivered_year
ORDER BY last_shipped_date;

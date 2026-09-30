SELECT YEAR(o.order_date) AS order_year, SORT_ARRAY(COLLECT_SET(s.status)) AS status_list, MAX(s.delivered_date) AS latest_delivered_date
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
WHERE o.store_id IS NULL
GROUP BY YEAR(o.order_date)
ORDER BY latest_delivered_date DESC;

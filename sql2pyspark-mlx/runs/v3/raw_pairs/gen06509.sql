SELECT YEAR(o.order_date) AS order_year, SORT_ARRAY(COLLECT_SET(s.status)) AS status_list
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
WHERE o.customer_id IN (9, 12)
GROUP BY YEAR(o.order_date)
ORDER BY order_year DESC;

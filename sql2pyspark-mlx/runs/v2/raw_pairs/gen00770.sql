SELECT YEAR(o.order_date) AS order_year, SORT_ARRAY(COLLECT_SET(s.carrier)) AS carrier_list, ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY YEAR(o.order_date)
ORDER BY order_year DESC;

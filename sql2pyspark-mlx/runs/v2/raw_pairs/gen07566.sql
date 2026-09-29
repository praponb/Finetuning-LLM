SELECT c.country, ROUND(STDDEV(o.discount), 2) AS stddev_discount, SORT_ARRAY(COLLECT_SET(o.channel)) AS channel_list
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE s.store_name NOT LIKE 'Si%'
GROUP BY c.country;

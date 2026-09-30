SELECT c.country, c.city, SORT_ARRAY(COLLECT_SET(o.channel)) AS channel_list
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.country, c.city;

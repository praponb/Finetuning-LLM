SELECT o.channel, MAX(s.cost) AS max_cost, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values FROM shipments s INNER JOIN orders o ON s.order_id = o.id GROUP BY o.channel

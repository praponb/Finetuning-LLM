SELECT o.status, o.channel, SORT_ARRAY(COLLECT_SET(s.carrier)) AS carrier_list
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY o.status, o.channel;

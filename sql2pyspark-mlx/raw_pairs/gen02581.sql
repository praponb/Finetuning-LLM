SELECT orders.channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(shipments.status))) AS status_values, SUM(CASE WHEN shipments.status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count
FROM shipments
JOIN orders ON shipments.order_id = orders.id
GROUP BY orders.channel
ORDER BY status_values DESC, in_transit_count

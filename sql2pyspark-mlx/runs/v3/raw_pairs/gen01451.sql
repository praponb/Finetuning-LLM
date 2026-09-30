SELECT o.channel, CASE WHEN o.shipping_fee >= 36 THEN 'high' WHEN o.shipping_fee >= 15 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
GROUP BY o.channel, CASE WHEN o.shipping_fee >= 36 THEN 'high' WHEN o.shipping_fee >= 15 THEN 'mid' ELSE 'low' END

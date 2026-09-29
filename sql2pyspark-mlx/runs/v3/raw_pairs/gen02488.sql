SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, CASE WHEN o.channel IN ('store', 'web') THEN 'group_a' ELSE 'other' END AS channel_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM'), CASE WHEN o.channel IN ('store', 'web') THEN 'group_a' ELSE 'other' END
ORDER BY order_month DESC, channel_group
LIMIT 5

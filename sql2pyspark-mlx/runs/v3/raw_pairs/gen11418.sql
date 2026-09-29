SELECT CASE WHEN o.channel IN ('partner', 'store') THEN 'core' ELSE 'other' END AS channel_group, COUNT(s.cost) AS cost_count, SORT_ARRAY(COLLECT_SET(s.status)) AS status_list
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY CASE WHEN o.channel IN ('partner', 'store') THEN 'core' ELSE 'other' END
ORDER BY channel_group

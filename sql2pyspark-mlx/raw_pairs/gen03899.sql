SELECT CASE WHEN event_type IN ('click', 'purchase') THEN 'core' ELSE 'other' END AS event_type_group, event_type, MAX(event_time) AS last_event_time
FROM web_events
WHERE TO_DATE(event_time) < '2023-10-01' AND page NOT IN ('/product', '/checkout', '/cart')
GROUP BY 1, 2;

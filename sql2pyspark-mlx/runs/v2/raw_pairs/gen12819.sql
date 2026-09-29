SELECT device, CASE WHEN device IN ('mobile', 'tablet') THEN 'Group A' ELSE 'Other' END AS device_group
FROM sessions
WHERE device IN ('desktop', 'mobile', 'tablet');

SELECT *, SUBSTRING(device, 1, 2) AS device_prefix, CASE WHEN device IN ('desktop', 'mobile') THEN 'Core' ELSE 'Other' END AS device_group
FROM sessions

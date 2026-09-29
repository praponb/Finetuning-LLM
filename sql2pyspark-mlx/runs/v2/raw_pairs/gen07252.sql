SELECT browser, customer_id, CASE WHEN device IN ('desktop', 'mobile') THEN 'Tier 1' ELSE 'Other' END AS device_group, TRIM(device) AS clean_device FROM sessions ORDER BY clean_device, browser DESC

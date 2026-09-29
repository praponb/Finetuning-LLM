SELECT id, device, CASE WHEN browser IN ('Chrome', 'Edge') THEN 'Group A' ELSE 'Other' END AS browser_group FROM sessions WHERE customer_id IS NULL OR country <> 'UK';

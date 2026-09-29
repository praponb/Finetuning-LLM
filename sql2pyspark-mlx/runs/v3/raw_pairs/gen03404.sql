SELECT browser, device, CASE WHEN device = 'desktop' THEN 1 ELSE 0 END AS is_desktop
FROM sessions
WHERE customer_id IN (7, 20) AND customer_id IS NULL;
